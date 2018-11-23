// Copyright 2017 The Hugo Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hugolib

import (
	"fmt"
	"github.com/gohugoio/hugo/helpers"
	radix "github.com/hashicorp/go-immutable-radix"
	"path"
	"strconv"
	"time"
)

// Sections returns the top level sections.
func (s *SiteInfo) Sections() Pages {
	home, err := s.Home()
	if err == nil {
		return home.Sections()
	}
	return nil
}

// Home is a shortcut to the home page, equivalent to .Site.GetPage "home".
func (s *SiteInfo) Home() (*Page, error) {
	return s.GetPage(KindHome)
}

// Parent returns a section's parent section or a page's section.
// To get a section's subsections, see Page's Sections method.
func (p *Page) Parent() *Page {
	return p.parent
}

// CurrentSection returns the page's current section or the page itself if home or a section.
// Note that this will return nil for pages that is not regular, home or section pages.
func (p *Page) CurrentSection() *Page {
	v := p
	if v.origOnCopy != nil {
		v = v.origOnCopy
	}
	if v.IsHome() || v.IsSection() {
		return v
	}

	return v.parent
}

// InSection returns whether the given page is in the current section.
// Note that this will always return false for pages that are
// not either regular, home or section pages.
func (p *Page) InSection(other interface{}) (bool, error) {
	if p == nil || other == nil {
		return false, nil
	}

	pp, err := unwrapPage(other)
	if err != nil {
		return false, err
	}

	if pp == nil {
		return false, nil
	}

	return pp.CurrentSection() == p.CurrentSection(), nil
}

// IsDescendant returns whether the current page is a descendant of the given page.
// Note that this method is not relevant for taxonomy lists and taxonomy terms pages.
func (p *Page) IsDescendant(other interface{}) (bool, error) {
	pp, err := unwrapPage(other)
	if err != nil {
		return false, err
	}

	if pp.Kind == KindPage && len(p.sections) == len(pp.sections) {
		// A regular page is never its section's descendant.
		return false, nil
	}
	return helpers.HasStringsPrefix(p.sections, pp.sections), nil
}

// IsAncestor returns whether the current page is an ancestor of the given page.
// Note that this method is not relevant for taxonomy lists and taxonomy terms pages.
func (p *Page) IsAncestor(other interface{}) (bool, error) {
	pp, err := unwrapPage(other)
	if err != nil {
		return false, err
	}

	if p.Kind == KindPage && len(p.sections) == len(pp.sections) {
		// A regular page is never its section's ancestor.
		return false, nil
	}

	return helpers.HasStringsPrefix(pp.sections, p.sections), nil
}

// Eq returns whether the current page equals the given page.
// Note that this is more accurate than doing `{{ if eq $page $otherPage }}`
// since a Page can be embedded in another type.
func (p *Page) Eq(other interface{}) bool {
	pp, err := unwrapPage(other)
	if err != nil {
		return false
	}

	return p == pp
}

func unwrapPage(in interface{}) (*Page, error) {
	if po, ok := in.(*PageOutput); ok {
		in = po.Page
	}

	pp, ok := in.(*Page)
	if !ok {
		return nil, fmt.Errorf("%T not supported", in)
	}
	return pp, nil
}

// Sections returns this section's subsections, if any.
// Note that for non-sections, this method will always return an empty list.
func (p *Page) Sections() Pages {
	return p.subSections
}

func (p *Page) SubSectionsPageIds() []string {
	return p.SubSectionsIds
}

func (p *Page) AllSectionNames() []string {
	return p.sections
}

func (p *Page) SectionsCount() int {
	count := 0

	if (p.sections != nil) {
		count = len(p.sections)
	}
	return count
}

func (p *Page) AllAboveSectionsPageIds() PageIds {
	return reverse(p.findAllAboveSectionsRec(p.ParentId))
}

func (p *Page) AllSubSectionsPageIds() PageIds {
	return p.findAllSubSectionsRec(p.SubSectionsIds)
}

func (p *Page) AllSubSectionsPagesPageIds() PageIds {

	if len(p.SubSectionsIds) == 0 {
		return p.PageIds
	}

	cache_items, found := p.s.PageStore.cache.Get("AllSubSectionsPagesPageIds" + string(p.ID))

	if found {
		return cache_items.(PageIds)
	}

	start_p := time.Now()
	pageIds := p.findAllSubSectionsPagesPageIdsRec(p.SubSectionsPageIds())
	pageIds = append(pageIds, p.PageIds...)

	if time.Now().Sub(start_p).Seconds() > 0.5 {
		elapsed := time.Since(start_p)
		fmt.Println("get all sections page page ids ", " ", p.ID, " ", p.Kind, " ", elapsed, " ", MyCaller())
	}

	p.s.PageStore.cache.SetDefault("AllSubSectionsPagesPageIds"+string(p.ID), pageIds)

	return pageIds
}

func (p *Page) findAllAboveSectionsRec(pageId PageId) PageIds {
	var allParents PageIds

	if pageId == "" {
		return allParents
	}

	page := p.s.PageStore.getActualPageById(pageId)

	if page.ParentId == "" {
		return allParents
	}

	pageParent := p.s.PageStore.getActualPageById(page.ParentId)

	if pageParent.Kind == KindHome {
		return append(allParents, PageId(page.ID))
	} else {
		allParents = append(allParents, PageId(page.ID))
		allParents = append(allParents, p.findAllAboveSectionsRec(page.ParentId)...)
	}

	return allParents
}

func reverse(ss PageIds) PageIds {
	last := len(ss) - 1
	for i := 0; i < len(ss)/2; i++ {
		ss[i], ss[last-i] = ss[last-i], ss[i]
	}

	return ss
}

func (p *Page) findAllSubSectionsRec(pageIds []string) PageIds {

	var allSections PageIds

	for _, pageId := range pageIds {
		page := p.s.PageStore.getActualPageById(PageId(pageId))
		if len(page.SubSectionsIds) > 0 {
			allSections = append(allSections, PageId(page.ID))
			allSections = append(allSections, p.findAllSubSectionsRec(page.SubSectionsIds)...)
		} else {
			return append(allSections, PageId(page.ID))
		}

	}

	return allSections
}

func (p *Page) findAllSubSectionsPagesPageIdsRec(pageIds []string) PageIds {
	var allSectionsPages PageIds

	for _, pageId := range pageIds {
		page := p.s.PageStore.getActualPageById(PageId(pageId))
		if len(page.SubSectionsIds) > 0 {
			allSectionsPages = append(allSectionsPages, page.PageIds...)
			allSectionsPages = append(allSectionsPages, p.findAllSubSectionsPagesPageIdsRec(page.SubSectionsIds)...)
		} else {
			allSectionsPages = append(allSectionsPages, page.PageIds...)
		}

	}
	return allSectionsPages
}

func (s *Site) assembleSections() Pages {
	var newPages Pages

	if !s.isEnabled(KindSection) {
		return newPages
	}

	// Maps section kind pages to their path, i.e. "my/section"
	sectionPages := make(map[string]*SectionGrouping)

	// The sections with content files will already have been created.
	sections := s.PageStore.findPagesByKindForSections(KindSection)

	for i, sect := range sections {
		sectPage := &sections[i]
		sectionPages[path.Join(sect.sections...)] = sectPage
	}

	const (
		sectKey     = "__hs"
		sectSectKey = "_a" + sectKey
		sectPageKey = "_b" + sectKey
	)

	var (
		inPages    = radix.New().Txn()
		inSections = radix.New().Txn()
		undecided  []*SectionGrouping
	)

	counter := 0
	home := s.PageStore.findFirstPageByKindIn(KindHome)

	//s.PageStore.printMemoryAndCaller("Before first each in sections")

	s.PageStore.eachPages(func(p *Page) (error) {
		if p.Kind != KindPage {
			return nil
		}

		if len(p.sections) == 0 {
			// Root level pages. These will have the home page as their Parent.
			p.ParentId = PageId(home.ID)
			return nil
		}

		sectionKey := path.Join(p.sections...)
		_, found := sectionPages[sectionKey]

		var sect *Page

		if !found && len(p.sections) == 1 {
			// We only create content-file-less sections for the root sections.
			sect = s.newSectionPage(p.sections[0])
			sectionPages[sectionKey] = &SectionGrouping{sections: sect.sections}
			newPages = append(newPages, sect)
			found = true
		}

		if len(p.sections) > 1 {
			// Create the root section if not found.
			_, rootFound := sectionPages[p.sections[0]]
			if !rootFound {
				sect = s.newSectionPage(p.sections[0])
				sectionPages[p.sections[0]] = &SectionGrouping{sections: sect.sections}
				newPages = append(newPages, sect)
			}
		}

		if found {
			pagePath := path.Join(sectionKey, sectPageKey, strconv.Itoa(counter))
			//inPages.Insert([]byte(pagePath), p.toSectionGrouping())
			p.pagePath = pagePath
		} else {
			panic("Found undecided pages")
			undecided = append(undecided, p.toSectionGrouping())
		}

		counter++

		return nil
	}, true)

	//s.PageStore.printMemoryAndCaller("After first each in sections")

	// Create any missing sections in the tree.
	// A sub-section needs a content file, but to create a navigational tree,
	// given a content file in /content/a/b/c/_index.md, we cannot create just
	// the c section.
	for _, sect := range sectionPages {
		for i := len(sect.sections); i > 0; i-- {
			sectionPath := sect.sections[:i]
			sectionKey := path.Join(sectionPath...)
			_, found := sectionPages[sectionKey]
			var sect *Page

			if !found {
				sect = s.newSectionPage(sectionPath[len(sectionPath)-1])
				sect.sections = sectionPath
				sectionPages[sectionKey] = &SectionGrouping{sections: sect.sections}
				newPages = append(newPages, sect)
			}
		}
	}

	for k, sect := range sectionPages {
		pagePath := path.Join(k, sectSectKey)
		//inPages.Insert([]byte(pagePath), sect)

		s.PageStore.updateField(sect.pageId, func(pageModel *PageModel) {
			pageModel.PagePath = pagePath
		})

		inSections.Insert([]byte(k), sect)
	}

	var (
		currentSection *Page
		children       PageIds
		rootSections   = inSections.Commit().Root()
	)

	for i, p := range undecided {
		// Now we can decide where to put this page into the tree.
		sectionKey := path.Join(p.sections...)
		_, v, _ := rootSections.LongestPrefix([]byte(sectionKey))
		sect := v.(*SectionGrouping)
		pagePath := path.Join(path.Join(sect.sections...), sectSectKey, "u", strconv.Itoa(i))
		inPages.Insert([]byte(pagePath), p)
	}

	//var rootPages = inPages.Commit().Root()

	//s.PageStore.printMemoryAndCaller("Before root walk")

	s.PageStore.eachPages(func(p *Page) (error) {

		//fmt.Println(string(p.pagePath))

		if p.Kind == KindSection {
			if currentSection != nil {
				// A new section
				s.PageStore.storePageIds(Page{
					ID:             currentSection.ID,
					PageIds:        children,
				})

			}

			currentSection = p
			children = make(PageIds, 0)

			return nil

		}

		// Regular page
		if currentSection != nil {
			p.ParentId = PageId(currentSection.ID)
		}

		children = append(children, PageId(p.ID))
		return nil
	}, true)

	if currentSection != nil {
		currentSection.PageIds = children
	}

	//s.PageStore.printMemoryAndCaller("After first each in sections")

	//sectRootPagesMap := make(map[PageId][]PageId)

	// Build the sections hierarchy
	for _, sect := range sectionPages {
		if len(sect.sections) == 1 {
			sect.parentId = PageId(home.ID)
			sect.parent = &SectionGrouping{pageId: PageId(home.ID)}
			//sectRootPagesMap[sect.ID] = [sect.ParentId
		} else {
			parentSearchKey := path.Join(sect.sections[:len(sect.sections)-1]...)

			_, v, _ := rootSections.LongestPrefix([]byte(parentSearchKey))
			p := v.(*SectionGrouping)
			sect.parentId = p.pageId
			//sectRootPagesMap[sect.ID] = sect.ParentId

			for _, section := range sectionPages {
				if PageId(section.pageId) == p.pageId {
					sect.parent = section
				}
			}
		}

		if sect.parentId != "" {
			if s.PageStore.pageExists(sect.parentId) {
				s.PageStore.storeSubSectionsPageIds(sect.parentId, []PageId{PageId(sect.pageId)})

				s.PageStore.updateField(sect.pageId, func(pageModel *PageModel) {
					pageModel.ParentId = sect.parentId
				})

			} else {
				panic("Page doesn't exists in section hierarchy")
				sect.parent.SubSectionsIds = append(sect.parent.SubSectionsIds, PageId(sect.pageId))
			}
		}

	}

	//s.PageStore.printMemoryAndCaller("Before second root walk")

	//s.PageStore.eachPages(func(p *Page) (error) {
	//
	//	if p.Kind == KindSection {
	//
	//		for _, section := range sectionPages {
	//			if PageId(section.pageId) == p.pageId {
	//				section.PageIds = p.childrenPageIds
	//
	//				//if section.saved {
	//				s.PageStore.updateField(p.pageId, func(pageModel *PageModel) {
	//					pageModel.ParentId = section.parentId
	//				})
	//
	//				subSectionIds := make([]string, 0)
	//
	//				for _, x := range section.SubSectionsIds {
	//					subSectionIds = append(subSectionIds, string(x))
	//				}
	//
	//				s.PageStore.storePageIds(Page{
	//					ID:             string(section.pageId),
	//					PageIds:        section.PageIds,
	//					SubSectionsIds: subSectionIds,
	//				})
	//				//}
	//
	//				return nil
	//			}
	//		}
	//
	//		return nil
	//	}
	//
	//	s.PageStore.updateField(p.pageId, func(pageModel *PageModel) {
	//		pageModel.ParentId = p.parentId
	//	})
	//
	//	s.PageStore.storeSubSectionsPageIds(p.pageId, p.childrenPageIds)
	//
	//	return nil
	//},true)

	//s.PageStore.printMemoryAndCaller("After second root walk")

	//TODO DAVID this is not needed in normal use
	//var (
	//	sectionsParamId      = "mainSections"
	//	sectionsParamIdLower = strings.ToLower(sectionsParamId)
	//	mainSections         interface{}
	//	mainSectionsFound    bool
	//	maxSectionWeight     int
	//)
	//
	//mainSections, mainSectionsFound = s.Info.Params[sectionsParamIdLower]
	//
	//for _, sect := range sectionPages {
	//	if sect.parent != nil {
	//		sect.parent.subSections.Sort()
	//	}
	//
	//	for i, p := range sect.Pages {
	//		if i > 0 {
	//			p.NextInSection = sect.Pages[i-1]
	//		}
	//		if i < len(sect.Pages)-1 {
	//			p.PrevInSection = sect.Pages[i+1]
	//		}
	//	}
	//
	//	if !mainSectionsFound {
	//		weight := len(sect.Pages) + (len(sect.Sections()) * 5)
	//		if weight >= maxSectionWeight {
	//			mainSections = []string{sect.Section()}
	//			maxSectionWeight = weight
	//		}
	//	}
	//}
	//
	//// Try to make this as backwards compatible as possible.
	//s.Info.Params[sectionsParamId] = mainSections
	//s.Info.Params[sectionsParamIdLower] = mainSections

	return newPages

}

func (p *Page) setPagePages(pages Pages) {
	pages.Sort()
	p.Pages = pages
	p.Data = make(map[string]interface{})
	p.Data["Pages"] = pages
}
