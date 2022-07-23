package db

import (
	"MSSQLParser/page"
	"fmt"
)

type Database struct {
	Pages  page.Pages
	Tables []Table
}

func (db Database) ProcessPage(bs []byte) page.Page {
	var page *page.Page = new(page.Page)
	page.Process(bs)

	return *page
}

func (db *Database) FilterByType(pageType string) {
	db.Pages = db.Pages.FilterByType(pageType) //mutable
}

func (db *Database) FilterBySystemTables(systemTables string) {
	db.Pages = db.Pages.FilterBySystemTables(systemTables)
}

func (db Database) createMap(tablename string) map[any]any {
	results := map[any]any{}
	systemPages := db.Pages.FilterBySystemTables(tablename)
	for _, tablePages := range systemPages {
		for _, tablePage := range tablePages {
			for _, datarow := range tablePage.DataRows {
				objectId, val := datarow.SystemTable.GetData()

				results[objectId] = val

			}
		}

	}
	return results
}

func (db Database) createMapList(tablename string) map[int32][]page.Result[string, string, uint16] {
	results := map[int32][]page.Result[string, string, uint16]{}
	systemPages := db.Pages.FilterBySystemTables(tablename)
	for _, tablePages := range systemPages {
		for _, tablePage := range tablePages {
			for _, datarow := range tablePage.DataRows {
				objectId, res := datarow.SystemTable.GetData()

				results[(objectId).(int32)] = append(results[(objectId).(int32)], res.(page.Result[string, string, uint16]))
			}

		}
	}
	return results
}

func (db Database) ShowTables(tablename string) {
	for _, table := range db.Tables {
		if table.Name != tablename {
			continue
		}
		table.printCols()
	}

}

func (db Database) GetTablesInformation() []Table {
	tablesMap := db.createMap("sysschobjs")
	colsMap := db.createMapList("syscolpars")
	tableAllocsMap := db.createMap("sysrowsets")
	tableSysAllocsMap := db.createMap("sysallocationunits")

	var tables []Table
	for tobjectId, tname := range tablesMap {
		results, ok := colsMap[tobjectId.(int32)]

		table := Table{Name: tname.(string), ObjectId: tobjectId.(int32)}

		if ok {

			table.addColumns(results)
		}
		partitionId, ok := tableAllocsMap[tobjectId]
		if ok {
			table.PartitionId = partitionId.(uint64)
		}

		pageObjetId, ok := tableSysAllocsMap[table.PartitionId]

		if ok {
			fmt.Printf("%d", pageObjetId)
			//table.getContent(db.Pages[pageObjetId.(uint32)])
		}

		tables = append(tables, table)
	}

	return tables

}
