package db

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

func (db Database) GetTablesInformation() {
	tablePages := db.Pages.FilterBySystemTables("sysschobjs")
	tableCols := db.Pages.FilterBySystemTables("syscolpars")
	tableAllocs := db.Pages.FilterBySystemTables("sysrowsets")
	tables := map[int32]string{}
	cols := map[int32][]string{}

	tableAlcs := map[int32]string{}

	for _, tablePage := range tablePages {
		for _, datarow := range tablePage.DataRows {
			tobjectId, tname := datarow.SystemTable.GetData()
			tables[tobjectId] = tname

		}
	}
	for _, tablecol := range tableCols {
		for _, datarow := range tablecol.DataRows {
			colid, colname := datarow.SystemTable.GetData()
			cols[colid] = append(cols[colid], colname)
		}
	}

	for _, tablealloc := range tableAllocs {
		for _, datarow := range tablealloc.DataRows {
			tobjecId, partitionId := datarow.SystemTable.GetData()
			tableAlcs[tobjecId] = partitionId
		}
	}

	for tobjectId, tname := range tables {
		cols, ok := cols[tobjectId]

		var table Table
		table.Name = tname
		if ok {
			table.addColumns(cols)
		}
		if len(table.Columns) != 0 {
			table.printCols()
		}

	}

}
