package hugolib

import (
	"time"
	"sync"
	"os"
	apex "github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/globalsign/mgo"
	"fmt"
	"gopkg.in/mgo.v2/bson"
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

	Site *Site

	MongoSession *mgo.Session

	SinceTime time.Time

	tempPages       NewPages
	tempRawPages    NewPages
	tempAllPages    NewPages
	tempUpdatePages []UpdatePage
	updateMutex     *sync.Mutex
}

func (ps *PageStore) initPageStore(site *Site) {

	url := "mongodb://localhost"
	ps.MongoSession, _ = mgo.Dial(url)

	ps.SinceTime = time.Now()
	ps.Site = site

	ps.tempPages = make(NewPages, 0)
	ps.tempRawPages = make(NewPages, 0)
	ps.tempAllPages = make(NewPages, 0)
	ps.tempUpdatePages = make([]UpdatePage, 0)

	ps.updateMutex = &sync.Mutex{}

	pwd, _ := os.Getwd()

	fi, _ := os.Create(pwd + "/tmp/memory.txt")

	handler := text.New(fi)
	apex.SetHandler(handler)

	ps.MongoSession.DB("hugo").C("test1").DropCollection()
}

type NewPages []*Page
type NewImmutablePages []Page
type PageIds []PageId
type PageId bson.ObjectId
type UpdatePage struct {
	DocType string
	Page    Page
}

func (ps *PageStore) AddToAllRawrPages(pages ...*Page) {

	var dataSlice = pages
	var interfaceSlice []interface{} = make([]interface{}, len(dataSlice))
	for i, p := range dataSlice {
		interfaceSlice[i] = ps.pageToPageModel(p)
	}

	err := ps.MongoSession.DB("hugo").C("test1").Insert(interfaceSlice...)

	if err != nil {
		fmt.Println(err.Error())
	}

}

func (ps *PageStore) eachRawPages(f func(*Page)) {
	item := PageModel{}
	items := ps.MongoSession.DB("hugo").C("test1").Find(bson.M{}).Batch(200).Iter()

	for items.Next(&item) {
		fmt.Println("Doing Item ", item.ID.String())
		page := ps.pageModelToPage(&item)
		page.s = ps.Site

		f(&page)
	}
}

func (ps *PageStore) pageToPageModel(p *Page) PageModel {
	var mainPageOutput PageOutput
	mainPageOutput = PageOutput{}

	if p.mainPageOutput != nil {
		mainPageOutput = *p.mainPageOutput
	}

	return PageModel{
		Kind:              p.Kind,
		PageIds:           p.PageIds,
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
		Plain:             p.plain,
		PlainWords:        p.plainWords,
		RenderingConfig:   p.renderingConfig,
		PageMenus:         p.pageMenus,
		Position:          p.Position,
		GitInfo:           p.GitInfo,
		Sections:          p.sections,
		ParentId:          p.ParentId,
		OrigOnCopyId:      p.origOnCopyId,
		SubSectionsIds:    p.SubSectionsIds,
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
	}
}

func (ps *PageStore) pageModelToPage(p *PageModel) Page {

	return Page{
		Kind:              p.Kind,
		PageIds:           p.PageIds,
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
		SubSectionsIds:    p.SubSectionsIds,
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

	}
}
