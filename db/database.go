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

func (db Database) createMapListGeneric(tablename string) map[any][]any {
	results := map[any][]any{}
	systemPages := db.Pages.FilterBySystemTables(tablename)
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
	systemPages := db.Pages.FilterBySystemTables(tablename)
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
	tablesMap := db.createMap("sysschobjs")
	colsMap := db.createMapList("syscolpars")
	tableAllocsMap := db.createMap("sysrowsets")
	tableSysAllocsMap := db.createMapListGeneric("sysallocationunits")

	var tables []Table
	for tobjectId, tname := range tablesMap {
		results, ok := colsMap[tobjectId.(int32)]
		fmt.Printf("Processing table %s with object id %d\n", tname, tobjectId)
		table := Table{Name: tname.(string), ObjectId: tobjectId.(int32)}

		if ok {

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
		partitionId, ok := tableAllocsMap[tobjectId]
		if ok {
			table.PartitionId = partitionId.(uint64)
		}

		pageObjectIds, ok := tableSysAllocsMap[table.PartitionId]

		if ok {
			for _, pageObjectId := range pageObjectIds {
				table.setContent(db.Pages[pageObjectId.(uint32)])
			}

		}

		tables = append(tables, table)
	}

	return tables

}
