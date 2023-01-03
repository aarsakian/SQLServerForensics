package db

import (
	"MSSQLParser/page"
	"fmt"
)

type Database struct {
	PagesMap page.PagesMap
	Tables   []Table
}

func (db Database) ProcessPage(bs []byte) page.Page {
	var page *page.Page = new(page.Page)
	page.Process(bs)

	return *page
}

func (db *Database) FilterBySystemTables(systemTables string) {
	db.PagesMap = db.PagesMap.FilterBySystemTables(systemTables)
}

func (db Database) createMap(tablename string) map[any]any {
	results := map[any]any{}
	systemPages := db.PagesMap.FilterBySystemTables(tablename)
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

func (db Database) createMapListGeneric(tablename string) map[any][]any {
	results := map[any][]any{}
	systemPages := db.PagesMap.FilterBySystemTables(tablename)
	for _, tablePages := range systemPages {
		for _, tablePage := range tablePages {
			for _, datarow := range tablePage.DataRows {
				objectId, val := datarow.SystemTable.GetData()

				results[objectId] = append(results[objectId], val)

			}
		}

	}
	return results
}

func (db Database) createMapList(tablename string) map[int32][]page.Result[string, string, uint16, uint16] {
	results := map[int32][]page.Result[string, string, uint16, uint16]{}
	systemPages := db.PagesMap.FilterBySystemTables(tablename)
	for _, tablePages := range systemPages {
		for _, tablePage := range tablePages {
			for _, datarow := range tablePage.DataRows {
				objectId, res := datarow.SystemTable.GetData()

				results[(objectId).(int32)] = append(results[(objectId).(int32)], res.(page.Result[string, string, uint16, uint16]))
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
		table.printSchema()
		table.printData()
	}

}

func (db Database) GetTablesInformation() []Table {
	tablesMap := db.createMap("sysschobjs")   // table information holds a map of object ids and table names
	colsMap := db.createMapList("syscolpars") //table objectid = name , type, size, colorder

	tableAllocsMap := db.createMap("sysrowsets")                       //(ttable objectid) = sysrowsets.Rowsetid
	tableSysAllocsMap := db.createMapListGeneric("sysallocationunits") //sysrowsets.Rowsetid =  OwnerId, page ObjectId

	var tables []Table
	for tobjectId, tname := range tablesMap {
		results, ok := colsMap[tobjectId.(int32)] // correlate table with its columns

		table := Table{Name: tname.(string), ObjectId: tobjectId.(int32)}

		if ok {
			fmt.Printf("Processing table %s with object id %d\n", tname, tobjectId)
			columns := table.addColumns(results)
			vid := 0 // keeps order var len cols
			//range copies values
			for idx := range columns {
				if columns[idx].isStatic() {
					columns[idx].VarLenOrder = 0
				} else {

					columns[idx].VarLenOrder = uint16(vid)
					vid++
				}

			}

		}

		rowsetId, ok := tableAllocsMap[tobjectId] // from sysrowsets idmajor => rowsetid
		if ok {
			table.Rowsetid = rowsetId.(uint64) // rowsetid
		}

		pageObjectIds, ok := tableSysAllocsMap[table.Rowsetid] // from sysallocunits rowsetid => page ObjectId
		var table_alloc_pages page.Pages
		if ok {
			for _, pageObjectId := range pageObjectIds {
				table_alloc_pages = append(table_alloc_pages, db.PagesMap[pageObjectId.(uint32)]...) // find the pages the table was allocated

			}
			dataPages := table_alloc_pages.FilterByTypeToMap("DATA")
			lobPages := table_alloc_pages.FilterByTypeToMap("LOB")
			table.setContent(dataPages, lobPages) // correlerate with page object ids

		}

		tables = append(tables, table)
	}

	return tables

}
