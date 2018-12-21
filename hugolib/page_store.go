package hugolib

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	apex "github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/gohugoio/hugo/config"
	"github.com/patrickmn/go-cache"
	"github.com/tecbot/gorocksdb"
	"html/template"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type PageStore struct {
	Pages NewPages

	// Includes all pages in all languages, including the current one.
	// Includes pages of all types.
	AllPages NewPages

	// A convenience cache for the traditional index types, taxonomies, home page etc.
	// This is for the current language only.
	indexPages NewPages

	// A convenience cache for the regular pages.
	// This is for the current language only.
	RegularPages NewPages

	// A convenience cache for the all the regular pages.
	AllRegularPages NewPages

	// Includes absolute all pages (of all types), including drafts etc.
	rawAllPages NewPages

	Site     *Site
	SiteInfo *SiteInfo
	Cfg      config.Provider

	cache *cache.Cache

	MongoSession *mgo.Session

	Redis    *redis.Client
	RocksDb  *gorocksdb.DB
	LRUCache *gorocksdb.Cache

	SinceTime time.Time

	PagesQueue []*Page

	tempPages       NewPages
	tempRawPages    NewPages
	tempAllPages    NewPages
	tempUpdatePages []UpdatePage
	updateMutex     *sync.Mutex
}

func (ps *PageStore) initPageStore(site *Site) {

	url := "mongodb://localhost"

	var aLogger *log.Logger
	f, _ := os.OpenFile("mongo.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

	aLogger = log.New(f, "", log.LstdFlags)

	mgo.SetLogger(aLogger)
	mgo.SetDebug(false)

	ps.MongoSession, _ = mgo.Dial(url)
	ps.MongoSession.SetSocketTimeout(1 * time.Hour)
	ps.MongoSession.SetPoolTimeout(1 * time.Hour)
	ps.MongoSession.SetCursorTimeout(0)
	ps.MongoSession.SetSyncTimeout(10 * time.Hour)

	ps.SinceTime = time.Now()
	ps.Site = site
	ps.SiteInfo = &site.Info

	ps.tempPages = make(NewPages, 0)
	ps.tempRawPages = make(NewPages, 0)
	ps.tempAllPages = make(NewPages, 0)
	ps.tempUpdatePages = make([]UpdatePage, 0)

	ps.updateMutex = &sync.Mutex{}

	ps.cache = cache.New(5*time.Hour, 10*time.Hour)

	pwd, _ := os.Getwd()

	fi, _ := os.Create(pwd + "/tmp/memory.txt")

	handler := text.New(fi)
	apex.SetHandler(handler)

	noReset := ps.Cfg.GetBool("noReset")

	ps.Redis = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   12})

	dbPath := ps.Cfg.GetString("rocketDbDir")

	if !noReset {

		fmt.Println("Mongo and redis reset")

		ps.MongoSession.DB("hugo").C("pages").DropCollection()
		ps.MongoSession.DB("hugo").C("pages_temp").DropCollection()
		ps.MongoSession.DB("hugo").C("raw_pages").DropCollection()
		ps.MongoSession.DB("hugo").C("weighted_pages").DropCollection()

		ps.CreateWeightedPagesIndesx()

		ps.Redis.FlushDB()

		if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
			os.RemoveAll(dbPath)
		}

	}

	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	lruCache := gorocksdb.NewLRUCache(1024 * 1024 * 100)

	ps.LRUCache = lruCache

	bbto.SetBlockCache(lruCache)
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	db, err := gorocksdb.OpenDb(opts, dbPath)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

	fmt.Println(db.GetProperty("rocksdb.estimate-table-readers-mem"))

	fmt.Println(lruCache.GetUsage())

	ps.RocksDb = db

	ps.PagesQueue = make([]*Page, 0)
}

func (ps *PageStore) CreateWeightedPagesIndesx() {
	index1 := mgo.Index{
		Key:        []string{"key"},
		Unique:     false,
		DropDups:   false,
		Background: true,
		Sparse:     false,
	}

	err := ps.MongoSession.DB("hugo").C("weighted_pages").EnsureIndex(index1)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
}
func (ps *PageStore) CreateMongoIndex() {
	index3 := mgo.Index{
		Key:        []string{"params.publishdate"},
		Unique:     false,
		DropDups:   false,
		Background: true,
		Sparse:     false,
	}

	ps.MongoSession.DB("hugo").C("pages").DropAllIndexes()

	err3 := ps.MongoSession.DB("hugo").C("pages").EnsureIndex(index3)
	//err4 := ps.MongoSession.DB("hugo").C("pages").EnsureIndex(index4)

	if err3 != nil {
		fmt.Println(err3.Error())
		panic(err3)
	}

}

func (ps *PageStore) CreateSectionsIndex() {

	index5 := mgo.Index{
		Key:        []string{"pagepath"},
		Unique:     false,
		DropDups:   false,
		Background: true,
		Sparse:     false,
	}

	ps.MongoSession.DB("hugo").C("pages").DropAllIndexes()

	err := ps.MongoSession.DB("hugo").C("pages").EnsureIndex(index5)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

}

type NewPages []*Page
type NewImmutablePages []Page
type PageIds []PageId
type PageId string

type UpdatePage struct {
	DocType string
	Page    Page
}

type SectionGrouping struct {
	pageId          PageId
	sections        []string
	Kind            string
	parentId        PageId
	childrenPageIds PageIds
	SubSectionsIds  PageIds
	PageIds         PageIds
	parent          *SectionGrouping
	saved           bool
}

func (ps *PageStore) AddToAllRawrPages(pages ...*Page) {

	var dataSlice = pages
	var interfaceSlice []interface{} = make([]interface{}, len(dataSlice))
	for i, p := range dataSlice {
		pageModel := ps.pageToPageModel(p)
		ps.storePageIds(*p)
		interfaceSlice[i] = pageModel
	}

	err := ps.MongoSession.DB("hugo").C("raw_pages").Insert(interfaceSlice...)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

}

func (ps *PageStore) AddToAllPagesWithBuffer(flush bool, pages ...*Page) {
	ps.PagesQueue = append(ps.PagesQueue, pages...)

	if len(ps.PagesQueue) >= 500 || flush {
		ps.AddToAllPages(ps.PagesQueue...)
		ps.PagesQueue = ps.PagesQueue[:0]
	}
}

func (ps *PageStore) AddToAllPages(pages ...*Page) {

	var dataSlice = pages
	var interfaceSlice []interface{} = make([]interface{}, len(dataSlice))
	for i, p := range dataSlice {
		pageModel := ps.pageToPageModel(p)
		ps.storePageIds(*p)
		interfaceSlice[i] = pageModel
	}

	if (len(interfaceSlice) == 0) {
		return
	}

	err := ps.MongoSession.DB("hugo").C("pages").Insert(interfaceSlice...)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

}

func (ps *PageStore) UpdatePagesWithNewCollection(collectionName string, updatePageIds bool, pages ...*Page) {

	var dataSlice = pages
	var interfaceSlice []interface{} = make([]interface{}, len(dataSlice))
	for i, p := range dataSlice {
		pageModel := ps.pageToPageModel(p)

		if updatePageIds {
			ps.storePageIds(*p)
		}

		interfaceSlice[i] = pageModel
	}

	if len(interfaceSlice) == 0 {
		return
	}

	err := ps.MongoSession.DB("hugo").C(collectionName).Insert(interfaceSlice...)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

}

func (ps *PageStore) AddToAllHeadlessPages(pages ...*Page) {

	var dataSlice = pages
	var interfaceSlice []interface{} = make([]interface{}, len(dataSlice))
	for i, p := range dataSlice {
		pageModel := ps.pageToPageModel(p)
		ps.storePageIds(*p)
		interfaceSlice[i] = pageModel
	}

	err := ps.MongoSession.DB("hugo").C("headless_pages").Insert(interfaceSlice...)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

}

func (ps *PageStore) AddWeightedPageIds(plural, key string, pws ...WeightedPage) {
	var dataSlice = pws
	var interfaceSlice []interface{} = make([]interface{}, len(dataSlice))
	for i, p := range dataSlice {
		id := fmt.Sprint(plural, "_", key, "_", p.ID)
		wp := WeightedPageIds{
			ID:     id,
			Weight: p.Weight,
			Key:    key,
			PageId: PageId(p.ID),
			Plural: plural,
			//Params: p.params,
		}

		//if plural == "searches" {
		//	terms := strings.Split(key, "--")
		//	searchKeysArray := terms[1:]
		//	wp.SearchKeys = searchKeysArray
		//	wp.Cardinality = len(searchKeysArray)
		//	wp.SearchLabel = terms[0]
		//}

		interfaceSlice[i] = wp

	}

	err := ps.MongoSession.DB("hugo").C("weighted_pages").Insert(interfaceSlice...)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
}

func (ps *PageStore) EachTaxonomiesKey(plural string, f func(key string)) {
	item := WeightedPageIds{}
	items := ps.MongoSession.DB("hugo").C("weighted_pages").Find(bson.M{"plural": plural}).Batch(3000).Iter()

	for items.Next(&item) {
		f(item.Key)
	}
}

func (ps *PageStore) eachRawPages(f func(*Page)) {
	start := time.Now()

	item := PageModel{}
	items := ps.MongoSession.DB("hugo").C("raw_pages").Find(bson.M{}).Batch(3000).Iter()

	for items.Next(&item) {
		//fmt.Println("Doing Item ", item.ID)

		page := ps.pageModelToPage(&item)

		ps.loadPageIds(&page)
		page.s = ps.Site
		f(&page)

		updatedPageModel := ps.pageToPageModel(&page)
		ps.storePageIds(page)
		ps.updatePage("raw_pages", updatedPageModel)

	}

	elapsed := time.Since(start)
	fmt.Println(" eachRawPages Took ", elapsed)
}

func (ps *PageStore) eachPages(f func(*Page) (error), update bool, loadPageIds bool, updatePageIds bool, createMongoIndex bool) {

	if ps.skipCallerFunc(MyCallerLastFunc(MyCaller())) {
		fmt.Println("Skipping ", MyCallerLastFunc(MyCaller()))
		return
	}

	fmt.Println(" eachPages start ", MyCaller(), " ", printMemory(), "Mb", " update pages ", update)

	start := time.Now()

	item := PageModel{}

	if createMongoIndex {
		ps.CreateMongoIndex()
	}

	items := ps.MongoSession.DB("hugo").C("pages").Find(bson.M{}).Batch(500).Iter()
	total := 0

	eachProgress := ps.Cfg.GetInt("printEachProgress")

	for items.Next(&item) {
		page := ps.pageModelToPage(&item)

		if loadPageIds {
			ps.loadPageIds(&page)
		}

		pageId := item.ID

		f(&page)

		total++

		if eachProgress > 0 && math.Mod(float64(total), float64(eachProgress)) == 0 {
			fmt.Println("eachPages process ", total, " ", MyCaller(), " ", printMemory(), "Mb", " update pages ", update, "rodb cach ", ps.LRUCache.GetUsage())
		}

		if update {
			page.ID = pageId
			ps.UpdatePagesWithNewCollection("pages_temp", updatePageIds, &page)
		}
	}

	if update {
		ps.MongoSession.DB("hugo").C("pages").DropCollection()
		err := ps.MongoSession.Run(bson.D{{"renameCollection", "hugo.pages_temp"}, {"to", "hugo.pages"}}, nil)

		if err != nil {
			fmt.Println(err.Error())
			panic(err)
		}

	}

	elapsed := time.Since(start)
	fmt.Println(" eachPages Took ", elapsed, " ", MyCaller(), " ", printMemory(), "Mb", " update pages ", update)
}

func (ps *PageStore) eachPagesWithSort(f func(*Page) (error), update bool) {
	if ps.skipCallerFunc(MyCallerLastFunc(MyCaller())) {
		fmt.Println("Skipping ", MyCallerLastFunc(MyCaller()))
		return
	}

	fmt.Println(" eachPages with sort start ", MyCaller(), " ", printMemory(), "Mb", " update pages ", update)

	start := time.Now()

	item := PageModel{}
	ps.CreateSectionsIndex()

	items := ps.MongoSession.DB("hugo").C("pages").Find(bson.M{}).Sort("+pagepath").Batch(3000).Prefetch(1).Iter()

	total := 0

	for items.Next(&item) {
		//fmt.Println("Doing Item ", item.ID)
		page := ps.pageModelToPage(&item)
		ps.loadPageIds(&page)
		pageId := item.ID

		//start_p := time.Now()
		f(&page)
		//if time.Now().Sub(start_p).Seconds() > 0.5 {
		//	elapsed := time.Since(start_p)
		//	fmt.Println("single page time ", page.ID, " ", page.Kind, " ", elapsed, " ", MyCaller())
		//}

		//fmt.Println("Doing page ", total)
		total++

		if update {
			//updatedPageModel := ps.pageToPageModel(&page)
			//updatedPageModel.ID = pageId
			//ps.storePageIds(page)
			//ps.updatePage("pages", updatedPageModel)
			page.ID = pageId
			ps.UpdatePagesWithNewCollection("pages_temp", false, &page)
		}
	}

	if update {
		ps.MongoSession.DB("hugo").C("pages").DropCollection()
		err := ps.MongoSession.Run(bson.D{{"renameCollection", "hugo.pages_temp"}, {"to", "hugo.pages"}}, nil)
		//err := ps.MongoSession.DB("hugo").Run(bson.D{{"copyTo": "pages"}}, nil)

		if err != nil {
			fmt.Println(err.Error())
			panic(err)
		}
	}

	elapsed := time.Since(start)
	fmt.Println(" eachPages Took ", elapsed, " ", MyCaller(), " ", printMemory(), "Mb", " update pages ", update)
}

func (ps *PageStore) skipCallerFunc(myLastCaller string) bool {
	skipEach := ps.Cfg.GetStringSlice("skipEach")

	return Contains(skipEach, myLastCaller)
}

func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func (ps *PageStore) countPages() int {

	count, _ := ps.MongoSession.DB("hugo").C("pages").Count()

	return count
}

func (ps *PageStore) countHeadlessPages() int {

	count, _ := ps.MongoSession.DB("hugo").C("headless_pages").Count()

	return count
}

func (ps *PageStore) updateField(pageId PageId, assigner func(pageModel *PageModel)) {

	pageModel := PageModel{}
	ps.MongoSession.DB("hugo").C("pages").FindId(pageId).One(&pageModel)
	assigner(&pageModel)

	err := ps.MongoSession.DB("hugo").C("pages").UpdateId(pageId, pageModel)
	//fmt.Println("Update ", pageId)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
}

func (ps *PageStore) savePage(pageId PageId, assigner func(pageModel *PageModel)) {

	pageModel := PageModel{}
	ps.MongoSession.DB("hugo").C("pages").FindId(pageId).One(&pageModel)
	assigner(&pageModel)

	err := ps.MongoSession.DB("hugo").C("pages").UpdateId(pageId, pageModel)
	//fmt.Println("Update ", pageId)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
}

func (ps *PageStore) pageExists(pageId PageId) bool {
	n, _ := ps.MongoSession.DB("hugo").C("pages").FindId(pageId).Count()

	return n > 0
}

func (ps *PageStore) eachHeadlessPages(f func(*Page)) {
	start := time.Now()
	item := PageModel{}
	items := ps.MongoSession.DB("hugo").C("headless_pages").Find(bson.M{}).Batch(200).Iter()

	for items.Next(&item) {
		//fmt.Println("Doing Item ", item.ID)
		page := ps.pageModelToPage(&item)
		ps.loadPageIds(&page)

		page.s = ps.Site

		f(&page)

		updatedPageModel := ps.pageToPageModel(&page)
		ps.storePageIds(page)
		ps.updatePage("headless_pages", updatedPageModel)
	}

	elapsed := time.Since(start)
	fmt.Println(" eachHeadlessPages Took ", elapsed)
}
func (ps *PageStore) eachPagesWithHeadless(f func(*Page) (error)) {
	item := PageModel{}
	items := ps.MongoSession.DB("hugo").C("pages").Find(bson.M{}).Batch(200).Iter()

	for items.Next(&item) {
		//fmt.Println("Doing Item ", item.ID)
		page := ps.pageModelToPage(&item)
		ps.loadPageIds(&page)
		page.s = ps.Site

		f(&page)

		updatedPageModel := ps.pageToPageModel(&page)
		ps.storePageIds(page)
		ps.updatePage("pages", updatedPageModel)
	}
}

func (ps *PageStore) getPageIdsByTermKey(plural string) PageIds {
	item := WeightedPageIds{}

	//cache_items, found := ps.cache.Get("getPageIdsByTermKey" + plural)

	//if found {
	//	return cache_items.([]PageId)
	//}

	items := ps.MongoSession.DB("hugo").C("weighted_pages").Find(bson.M{"plural": plural}).Batch(1000).Iter()

	pageIds := make(PageIds, 0)

	for items.Next(&item) {
		pageIds = append(pageIds, item.PageId)
	}

	//ps.cache.SetDefault(plural, pageIds)

	return pageIds
}

func (ps *PageStore) getPageIdsByTaxonomyKey(plural string, term string) PageIds {
	item := WeightedPageIds{}

	items := ps.MongoSession.DB("hugo").C("weighted_pages").Find(bson.M{"plural": plural, "key": term}).Batch(1000).Iter()

	pageIds := make(PageIds, 0)

	for items.Next(&item) {
		pageIds = append(pageIds, item.PageId)
	}

	return pageIds
}

func (ps *PageStore) storePageIds(page Page) {

	if len(page.PageIds) > 0 {
		pageIds := make([]string, 0)

		for _, x := range page.PageIds {
			pageIds = append(pageIds, string(x))
		}

		pageIdsJson, _ := json.Marshal(pageIds)

		//ps.Redis.SAdd(page.ID+"_PageIds", pageIds)
		//fmt.Println("Written to redis pageIds:",resultPageIds)
		ps.RDBSet(page.ID+"_PageIds", string(pageIdsJson))
	}

	if len(page.SubSectionsIds) > 0 {
		subSectionsIdsJson, _ := json.Marshal(page.SubSectionsIds)

		ps.RDBSet(page.ID+"_SubSectionsIds", string(subSectionsIdsJson))
	}
}

func (ps *PageStore) storeSubSectionsPageIds(pageId PageId, subSectionsPageIds PageIds) {
	if len(subSectionsPageIds) > 0 {
		pageIds := make([]string, 0)

		for _, x := range subSectionsPageIds {
			pageIds = append(pageIds, string(x))
		}

		pageSubSectionIdsResultJson := ps.RDBGet(string(pageId) + "_SubSectionsIds")

		pageSubSectionIdsResult := make([]PageId, 0)

		json.Unmarshal([]byte(pageSubSectionIdsResultJson), &pageSubSectionIdsResult)

		subSectionsIdsJson, _ := json.Marshal(append(subSectionsPageIds, pageSubSectionIdsResult...))

		ps.RDBSet(string(pageId)+"_SubSectionsIds", string(subSectionsIdsJson))
	}
}

func (ps *PageStore) loadPageIds(page *Page) {

	start_p := time.Now()
	pageIdsResultJson := ps.RDBGet(page.ID + "_PageIds")

	pageIdsResult := make([]PageId, 0)

	json.Unmarshal([]byte(pageIdsResultJson), &pageIdsResult)

	for _, x := range pageIdsResult {
		page.PageIds = append(page.PageIds, PageId(x))
	}
	page.PageIdsCount = len(page.PageIds)

	//fmt.Println("Found ", len(page.PageIds), " for page ", page.ID)
	pageSubSectionIdsResultJson := ps.RDBGet(page.ID + "_SubSectionsIds")

	pageSubSectionIdsResult := make([]string, 0)

	json.Unmarshal([]byte(pageSubSectionIdsResultJson), &pageSubSectionIdsResult)

	for _, x := range pageSubSectionIdsResult {
		page.SubSectionsIds = append(page.SubSectionsIds, x)
	}
	page.SubSectionsIdsCount = len(page.SubSectionsIds)

	elapsed := time.Since(start_p)
	fmt.Println("Redis get ids ", page.ID, " ", page.Kind, " ", elapsed, " ", MyCaller())

}

func (ps *PageStore) pageToPageModel(p *Page) PageModel {
	var mainPageOutput PageOutput
	mainPageOutput = PageOutput{}

	if p.mainPageOutput != nil {
		mainPageOutput = *p.mainPageOutput
	}

	//var shortCodeOrderedMap orderedMapMongo
	//
	//if p.shortcodeState != nil && p.shortcodeState.shortcodes.Len() > 0 {
	//
	//	keys := make([]string,0)
	//	for _,v := range p.shortcodeState.shortcodes.keys {
	//		keys = append(keys,v.(string))
	//	}
	//	shortCodeOrderedMap = orderedMapMongo{
	//		Keys: keys,
	//		M: p.shortcodeState.shortcodes.m,
	//	}
	//} else {
	//	shortCodeOrderedMap = orderedMapMongo{}
	//}

	return PageModel{
		Kind:              p.Kind,
		Resources:         p.Resources,
		ResourcesMetadata: p.resourcesMetadata,
		TranslationsIds:   p.translationsIds,
		TranslationKey:    p.translationKey,
		Params:            p.params,
		ContentV:          p.contentv,
		Summary:           p.summary,
		TableOfContents:   p.TableOfContents,
		Aliases:           p.Aliases,
		Images:            p.Images,
		Videos:            p.Videos,
		Truncated:         p.truncated,
		Draft:             p.Draft,
		Status:            p.Status,
		PageMeta:          p.PageMeta,
		Markup:            p.Markup,
		Extension:         p.extension,
		ContentType:       p.contentType,
		Renderable:        p.renderable,
		Layout:            p.Layout,
		SelfLayout:        p.selfLayout,
		LinkTitle:         p.linkTitle,
		Frontmatter:       p.frontmatter,
		RawContent:        p.rawContent,
		WorkContent:       p.workContent,
		IsCJKLanguage:     p.isCJKLanguage,
		ShortcodeState:    p.shortcodeState,
		//ShortCodeOrderedMap: shortCodeOrderedMap,
		Plain:             p.plain,
		PlainWords:        p.plainWords,
		RenderingConfig:   p.renderingConfig,
		PageMenus:         p.pageMenus,
		Position:          p.Position,
		GitInfo:           p.GitInfo,
		Sections:          p.sections,
		ParentId:          p.ParentId,
		OrigOnCopyId:      p.origOnCopyId,
		Title:             p.title,
		Description:       p.Description,
		Keywords:          p.Keywords,
		Data:              p.Data,
		PageDates:         p.PageDates,
		Sitemap:           p.Sitemap,
		UrlPath:           p.URLPath,
		FrontMatterURL:    p.frontMatterURL,
		PermaLink:         p.permalink,
		RelPermalink:      p.relPermalink,
		RelTargetPathBase: p.relTargetPathBase,
		ResourcePath:      p.resourcePath,
		Headless:          p.headless,
		LayoutDescriptor:  p.layoutDescriptor,
		Lang:              p.lang,
		OutputFormats:     p.outputFormats,
		MainPageOutput:    mainPageOutput,
		FileName:          p.SourceFileName,
		ID:                p.ID,
		PagePath:          p.pagePath,
	}
}

func (ps *PageStore) pageModelToPage(p *PageModel) Page {

	page := Page{
		Kind:              p.Kind,
		Resources:         p.Resources,
		resourcesMetadata: p.ResourcesMetadata,
		translationsIds:   p.TranslationsIds,
		translationKey:    p.TranslationKey,
		params:            p.Params,
		contentv:          p.ContentV,
		summary:           p.Summary,
		TableOfContents:   p.TableOfContents,
		Aliases:           p.Aliases,
		Images:            p.Images,
		Videos:            p.Videos,
		truncated:         p.Truncated,
		Draft:             p.Draft,
		Status:            p.Status,
		PageMeta:          p.PageMeta,
		Markup:            p.Markup,
		extension:         p.Extension,
		contentType:       p.ContentType,
		renderable:        p.Renderable,
		Layout:            p.Layout,
		selfLayout:        p.SelfLayout,
		linkTitle:         p.LinkTitle,
		frontmatter:       p.Frontmatter,
		rawContent:        p.RawContent,
		workContent:       p.WorkContent,
		isCJKLanguage:     p.IsCJKLanguage,
		shortcodeState:    p.ShortcodeState,
		plain:             p.Plain,
		plainWords:        p.PlainWords,
		renderingConfig:   p.RenderingConfig,
		pageMenus:         p.PageMenus,
		Position:          p.Position,
		GitInfo:           p.GitInfo,
		sections:          p.Sections,
		ParentId:          p.ParentId,
		origOnCopyId:      p.OrigOnCopyId,
		title:             p.Title,
		Description:       p.Description,
		Keywords:          p.Keywords,
		Data:              p.Data,
		PageDates:         p.PageDates,
		Sitemap:           p.Sitemap,
		URLPath:           p.UrlPath,
		frontMatterURL:    p.FrontMatterURL,
		permalink:         p.PermaLink,
		relPermalink:      p.RelPermalink,
		relTargetPathBase: p.RelTargetPathBase,
		resourcePath:      p.ResourcePath,
		headless:          p.Headless,
		layoutDescriptor:  p.LayoutDescriptor,
		lang:              p.Lang,
		outputFormats:     p.OutputFormats,
		mainPageOutput:    &p.MainPageOutput,
		ID:                p.ID,
		SourceFileName:    p.FileName,
		saved:             true,
		pagePath:          p.PagePath,
	}

	page.Source = Source{File: newFileInfo(
		ps.Site.SourceSpec,
		ps.Site.absContentDir(),
		p.FileName,
		nil,
		bundleNot,
	)}

	page.s = ps.Site
	page.Site = ps.SiteInfo
	page.pageInit = &pageInit{}

	page.shortcodeState = newShortcodeHandler(&page)
	//page.shortcodeState.shortcodes = &orderedMap{
	//	m: p.ShortCodeOrderedMap.M,
	//	keys: p.ShortCodeOrderedMap.Keys,
	//}
	page.initTargetPathDescriptor()
	page.pageContentInit = &pageContentInit{}

	//if p.ParentId != "" {
	//	page.parent = ps.getPageById(p.ParentId)
	//}

	return page
}

type ActualPages []Page

func (ps *PageStore) findPagesByKind(kind string) ActualPages {
	pages := make(ActualPages, 0)

	items := ps.MongoSession.DB("hugo").C("pages").Find(bson.M{"kind": kind}).Batch(200).Iter()
	item := PageModel{}
	for items.Next(&item) {
		page := ps.pageModelToPage(&item)
		ps.loadPageIds(&page)

		pages = append(pages, page)
	}

	return pages
}

type PageForSections struct {
	Sections []string
}

func (ps *PageStore) findPagesByKindForSections(kind string) []SectionGrouping {
	pages := make([]SectionGrouping, 0)

	items := ps.MongoSession.DB("hugo").C("pages").Find(bson.M{"kind": kind}).Batch(200).Iter()
	item := PageModel{}
	for items.Next(&item) {
		sectionGrouping := SectionGrouping{
			pageId:   PageId(item.ID),
			sections: item.Sections,
			Kind:     item.Kind,
			parentId: item.ParentId,
		}
		pages = append(pages, sectionGrouping)
	}

	return pages
}

func (ps *PageStore) findFirstPageByKindIn(kind string) Page {
	return ps.findPagesByKind(kind)[0]
}

func (ps *PageStore) findSectionsForGrouping() []SectionGrouping {
	sectionGroupings := make([]SectionGrouping, 1)
	sectionGroupings = append(sectionGroupings, SectionGrouping{})
	return sectionGroupings
}

func (p *Page) toSectionGrouping() *SectionGrouping {
	return &SectionGrouping{
		pageId:   PageId(p.ID),
		sections: p.sections,
		Kind:     p.Kind,
	}
}

func (ps *PageStore) updatePage(collection string, pageModel PageModel) {
	err := ps.MongoSession.DB("hugo").C(collection).UpdateId(pageModel.ID, pageModel)

	if err != nil {
		fmt.Println(err.Error() + " " + pageModel.ID)
		panic(err)
	}
}

func (ps *PageStore) getPageById(pageId PageId) *Page {
	pageModel := PageModel{}

	err := ps.MongoSession.DB("hugo").C("pages").FindId(pageId).One(&pageModel)

	if err != nil {
		panic(err)
	}

	page := ps.pageModelToPage(&pageModel)
	ps.loadPageIds(&page)

	return &page
}

func (ps *PageStore) getPagesById(pageIds PageIds) Pages {

	where := bson.M{"_id": bson.M{"$in": pageIds}}

	var results []PageModel

	//start_p := time.Now()
	err := ps.MongoSession.DB("hugo").C("pages").Find(where).All(&results)

	//elapsed := time.Since(start_p)
	//fmt.Println("Bulk get pages took ", " ", elapsed, " ", MyCaller())

	if err != nil {
		fmt.Println("Error when getting pageIds ", pageIds, " error: ", err.Error())
		panic(err)
	}

	pages := make(Pages, 0)

	for _, pm := range results {
		pageP := ps.pageModelToPage(&pm)
		ps.loadPageIds(&pageP)
		pages = append(pages, &pageP)
	}

	return pages
}

func (ps *PageStore) getActualPageById(pageId PageId) Page {
	pageModel := PageModel{}
	err := ps.MongoSession.DB("hugo").C("pages").FindId(pageId).One(&pageModel)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

	page := ps.pageModelToPage(&pageModel)
	ps.loadPageIds(&page)

	return page
}

func (ps *PageStore) getPageIds(bsonMap bson.M, sortFields []string) PageIds {

	pageIds := make(PageIds, 0)
	items := ps.MongoSession.DB("hugo").C("pages").Find(bsonMap).Sort(sortFields...).Select(bson.M{"_id": 1}).Batch(200).Iter()

	item := PageModel{}
	for items.Next(&item) {
		pageIds = append(pageIds, PageId(item.ID))
	}

	return pageIds
}

func (ps *PageStore) addPageIds(p *Page) {
	var pageIds = make(PageIds, 0)

	for i := 1; i <= 4*1000000; i++ {
		pageIds = append(pageIds, RandomString(40))
	}

	p.PageIds = pageIds
}

type WeightedPagePipe struct {
	Count       int
	ID          string `bson:"_id"`
	SearchLabel string
	SearchKeys  []string
}

type WeightedPagePipes []WeightedPagePipe

func (ps *PageStore) taxonomyTermsByCount(plural string) []WeightedPagePipe {

	//cache_items, found := ps.cache.Get("taxonomyTermsByCount" + plural)
	//
	//if found {
	//	return cache_items.([]WeightedPagePipe)
	//}

	start := time.Now()
	pipe := []bson.M{bson.M{"$match": bson.M{"plural": plural}}, bson.M{"$group": bson.M{"_id": "$key", "count": bson.M{"$sum": 1}}}, bson.M{"$sort": bson.M{"count": -1}}}

	items := ps.MongoSession.DB("hugo").C("weighted_pages").Pipe(pipe).Iter()
	weightedPagePipes := make([]WeightedPagePipe, 0)

	item := WeightedPagePipe{}
	for items.Next(&item) {
		weightedPagePipes = append(weightedPagePipes, item)
	}

	elapsed := time.Since(start)
	fmt.Println(" term count Took ", elapsed, " ", MyCaller())

	return weightedPagePipes
}

func (ps *PageStore) taxonomyTermsWithBsonMByCount(bsonM bson.M) []WeightedPagePipe {
	//start := time.Now()
	pipe := []bson.M{bson.M{"$match": bsonM}, bson.M{"$group": bson.M{"_id": "$key", "searchlabel": bson.M{"$first": "$searchlabel"}, "searchkeys": bson.M{"$first": "$searchkeys"}, "count": bson.M{"$sum": 1}}}, bson.M{"$sort": bson.M{"count": -1}}}

	items := ps.MongoSession.DB("hugo").C("weighted_pages").Pipe(pipe).Iter()
	weightedPagePipes := make([]WeightedPagePipe, 0)

	item := WeightedPagePipe{}
	for items.Next(&item) {
		weightedPagePipes = append(weightedPagePipes, item)
	}

	//elapsed := time.Since(start)
	//fmt.Println(" term count with bson Took ", elapsed, " ", MyCaller())

	return weightedPagePipes
}

func (ps *PageStore) getHomePage() *Page {

	pageModel := PageModel{}
	err := ps.MongoSession.DB("hugo").C("pages").FindId("home_").One(&pageModel)

	if err != nil && err.Error() == "not found" {
		return nil
	}

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

	page := ps.pageModelToPage(&pageModel)
	ps.loadPageIds(&page)

	return &page
}

func (ps *PageStore) getPageByHumanId(humanId string) *Page {

	pageModel := PageModel{}
	err := ps.MongoSession.DB("hugo").C("pages").Find(bson.M{"params.page_human_id": humanId}).One(&pageModel)

	if err != nil && err.Error() == "not found" {
		return nil
	}

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

	page := ps.pageModelToPage(&pageModel)
	ps.loadPageIds(&page)

	return &page
}

func (ps *PageStore) getPagesByHumanIds(humanIds []string) Pages {

	var results []PageModel
	err := ps.MongoSession.DB("hugo").C("pages").Find(bson.M{"params.page_human_id": bson.M{"$in": humanIds}}).All(&results)

	if err != nil && err.Error() == "not found" {
		return nil
	}

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

	pages := make(Pages, 0)

	for _, pm := range results {
		pageP := ps.pageModelToPage(&pm)
		ps.loadPageIds(&pageP)
		pages = append(pages, &pageP)
	}

	return pages
}

func (ps *PageStore) setPagePermalinkByPageHumanId(humanId string, permalink string) {
	ps.cache.SetDefault(humanId+"_permalink", permalink)
}

type LitePage struct {
	Permalink        string        `json:"p,omitempty"`
	Title            string        `json:"t,omitempty"`
	Summary          template.HTML `json:"s,omitempty"`
	Description      string        `json:"d,omitempty"`
	Image            string        `json:"i,omitempty"`
	TotalReviewCount float64       `json:"t2,omitempty"`
	StarsClass       string        `json:"s2,omitempty"`
	Price            float64       `json:"p2,omitempty"`
	Truncated        bool          `json:"t3,omitempty"`
	Tags             []string      `json:"t4,omitempty"`
	MasterVariation  bool          `json:"m,omitempty"`
}

func (ps *PageStore) setLitePageById(prefix string, id string, page *Page) {
	litePage := LitePage{
		Permalink:   page.Permalink(),
		Title:       page.Title(),
		Summary:     page.Summary(),
		Description: page.Description,
		Truncated:   page.Truncated(),
	}

	if val, ok := page.params["image"]; ok && val != nil {
		litePage.Image = val.(string);
	}

	if val, ok := page.params["total_review_count"]; ok && val != nil {
		litePage.TotalReviewCount = val.(float64);
	}

	if val, ok := page.params["stars_class"]; ok && val != nil {
		litePage.StarsClass = val.(string);
	}

	if val, ok := page.params["price"]; ok && val != nil {
		litePage.Price = val.(float64);
	}
	if val, ok := page.params["master_variation"]; ok && val != nil {
		litePage.MasterVariation = val.(bool);
	}

	if val, ok := page.params["tags"]; ok && val != nil {
		stringList := make([]string, 0)

		for _, x := range val.([]interface{}) {
			stringList = append(stringList, x.(string))
		}

		litePage.Tags = stringList
	}

	listPageJson, err := json.Marshal(litePage)

	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	ps.RDBSet(prefix+"_"+id, string(listPageJson))
}

func (ps *PageStore) getLitePageByHumanId(humanId string) *LitePage {

	//litePageBytes, _ := ps.Redis.Get("lite_" + humanId).Result()
	litePageBytes := ps.RDBGet("lite_" + humanId)

	if len(litePageBytes) == 0 {
		return nil
	}

	var litePage LitePage
	json.Unmarshal([]byte(litePageBytes), &litePage)

	return &litePage
}

func (ps *PageStore) getLitePageById(humanId string) *LitePage {
	litePageBytes := ps.RDBGet("id_" + humanId)

	if len(litePageBytes) == 0 {
		return nil
	}

	var litePage LitePage
	json.Unmarshal([]byte(litePageBytes), &litePage)

	return &litePage
}

func (ps *PageStore) getLitePagesById(humanIds PageIds) []LitePage {

	multiKeys := make([]string, 0)

	for _, v := range humanIds {
		multiKeys = append(multiKeys, "id_"+string(v))
	}

	litePageArray := ps.RDBMGet(multiKeys...)

	if len(litePageArray) == 0 {
		return nil
	}

	litePages := make([]LitePage, 0)

	for _, v := range litePageArray {
		var litePage LitePage
		json.Unmarshal([]byte(v), &litePage)

		litePages = append(litePages, litePage)
	}

	return litePages
}

func (ps *PageStore) getPagePermalinkByPageHumanId(humanId string) string {
	panic("Reimplement with lite pages")
	permalink, found := ps.cache.Get(humanId + "_permalink")
	if !found {
		return ""
	}

	return permalink.(string)
}

func (ps *PageStore) RDBGet(key string) string {
	ro := gorocksdb.NewDefaultReadOptions()
	slice, err := ps.RocksDb.Get(ro, []byte( key))

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	defer slice.Free()
	return string(slice.Data())

}

func (ps *PageStore) RDBMGet(keys ... string) []string {
	ro := gorocksdb.NewDefaultReadOptions()

	byteKeys := make([][]byte, 0)

	for _, x := range keys {
		byteKeys = append(byteKeys, []byte(x))
	}

	slices, err := ps.RocksDb.MultiGet(ro, byteKeys...)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

	returnStrings := make([]string, 0)

	for _, x := range slices {
		returnStrings = append(returnStrings, string(x.Data()))
		x.Free()
	}

	return returnStrings

}
func (ps *PageStore) RDBSet(key string, value string) {
	wo := gorocksdb.NewDefaultWriteOptions()
	ps.RocksDb.Put(wo, []byte( key), []byte(value))
}

func (ps *PageStore) startDebug() {
	mgo.SetDebug(true)
}

func (ps *PageStore) stoptDebug() {
	mgo.SetDebug(false)
	mgo.SetDebug(false)
}

func generatePageId(kind string, parts ...string) string {
	id := fmt.Sprint(kind, "_", strings.Join(parts, "_"))

	return id
}

func getMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}
func printMemory() string {

	debug.FreeOSMemory()

	var mem runtime.MemStats

	runtime.ReadMemStats(&mem)

	memory := mem.Alloc / 1024 / 1024

	return fmt.Sprint(memory)

	//f2, _ := os.Create("tmp/mem" + strconv.FormatUint(memory, 10) + ".prof")

	//apex.WithFields(apex.Fields{"memory": memory}).Info("Memory used")

	//if err := pprof.WriteHeapProfile(f2); err != nil {
	//	log.Fatal("could not start Memory profile: ", err)
	//}
	//
	//f2.Close()

}

func RandomString(n int) PageId {
	var letter = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return PageId(string(b))
}

func MyCaller() string {

	// we get the callers as uintptrs - but we just need 1
	fpcs := make([]uintptr, 1)

	// skip 3 levels to get to the caller of whoever called Caller()
	n := runtime.Callers(3, fpcs)
	if n == 0 {
		return "n/a" // proper error her would be better
	}

	// get the info of the actual function that's in the pointer
	fun := runtime.FuncForPC(fpcs[0] - 1)
	if fun == nil {
		return "n/a"
	}

	// return its name
	return fun.Name()
}

func MyCallerLastFunc(myCaller string) string {

	splitCaller := strings.Split(myCaller, ".")

	return splitCaller[len(splitCaller)-1]
}

func (ps *PageStore) printMemoryAndCaller(prefix string) {
	fmt.Println(prefix+" ", MyCaller(), " ", printMemory(), "Mb")
}
