package database

import (
	"MSSQLParser/page"
)

type Database struct {
	Pages page.Pages
}

func (db Database) ProcessPage(bs []byte) page.Page {
	var page *page.Page = new(page.Page)
	page.Process(bs)

	return *page
}

func (db *Database) FilterPagesByType(pageType string) {
	db.Pages = db.Pages.FilterByType(pageType)
}
