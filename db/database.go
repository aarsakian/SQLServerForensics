package db

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"fmt"
)

type Database struct {
	Name     string
	PagesMap page.PagesMap
	Tables   []Table
}

func (db Database) ProcessPage(bs []byte, offset int) page.Page {
	var page *page.Page = new(page.Page)
	page.Process(bs, offset)

	return *page
}

func (db *Database) FilterBySystemTables(systemTables string) {
	db.PagesMap = db.PagesMap.FilterBySystemTables(systemTables)
}

func (db Database) createMap(tablename string) map[any]page.Result[string, string, uint64, uint] {
	results := map[any]page.Result[string, string, uint64, uint]{}
	systemPages := db.PagesMap.FilterBySystemTables(tablename)
	for _, tablePages := range systemPages {
		for _, tablePage := range tablePages {
			for _, datarow := range tablePage.DataRows {
				objectId, res := datarow.SystemTable.GetData()
				results[objectId] = res.(page.Result[string, string, uint64, uint])

			}
		}

	}
	return results
}

func (db Database) createMapListGeneric(tablename string) map[any][]int32 {
	results := map[any][]int32{}
	systemPages := db.PagesMap.FilterBySystemTables(tablename)
	for _, tablePages := range systemPages {
		for _, tablePage := range tablePages {
			for _, datarow := range tablePage.DataRows {
				objectId, val := datarow.SystemTable.GetData()

				results[objectId] = append(results[objectId], val.(int32))

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

func (db Database) ShowTables(tablename string, showSchema bool, showContent bool,
	showAllocation bool, tabletype string) {
	tableLocated := false
	for _, table := range db.Tables {

		if table.Name != tablename && tablename != "all" {

			continue
		}

		if tabletype != "" && tabletype != "user" && table.Type != "User Table" {
			continue
		}

		if showSchema {
			table.printSchema()
		}
		if showContent {
			table.printData()
		}

		if showAllocation {
			pageIds := make(map[uint32]string, 0)
			for _, pageObjecId := range table.PageObjectIds {
				pages := db.PagesMap[pageObjecId]
				for _, page := range pages {
					pageIds[page.Header.PageId] = page.GetType()
				}
			}
			table.printAllocation(pageIds)
		}
		tableLocated = true

	}
	if !tableLocated {
		fmt.Printf("Table %s was not found! \n", tablename)
	}

}

func (db Database) GetTablesInformation() []Table {
	tablesMap := db.createMap("sysschobjs")   // table information holds a map of object ids and table names
	colsMap := db.createMapList("syscolpars") //table objectid = name , type, size, colorder

	tableAllocsMap := db.createMap("sysrowsets")                       //(ttable objectid) = sysrowsets.Rowsetid
	tableSysAllocsMap := db.createMapListGeneric("sysallocationunits") //sysrowsets.Rowsetid =  OwnerId, page ObjectId

	var tables []Table
	for tobjectId, res := range tablesMap {
		tname := res.First

		results, ok := colsMap[tobjectId.(int32)] // correlate table with its columns

		table := Table{Name: tname, ObjectId: tobjectId.(int32), Type: res.Second}

		msg := fmt.Sprintf("reconstructing table %s  objectId %d type %s", table.Name, table.ObjectId, table.Type)
		mslogger.Mslogger.Info(msg)

		if ok {
			//		fmt.Printf("Processing table %s with object id %d\n", tname, tobjectId)
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

		res, ok := tableAllocsMap[tobjectId] // from sysrowsets idmajor => rowsetid
		if ok {
			table.Rowsetid = uint64(res.Third) // rowsetid
		}

		pageObjectIds, ok := tableSysAllocsMap[table.Rowsetid] // from sysallocunits rowsetid => page ObjectId
		var table_alloc_pages page.Pages
		if ok {
			for _, pageObjectId := range pageObjectIds {
				table_alloc_pages = append(table_alloc_pages, db.PagesMap[pageObjectId]...) // find the pages the table was allocated

			}
			dataPages := table_alloc_pages.FilterByTypeToMap("DATA")
			lobPages := table_alloc_pages.FilterByTypeToMap("LOB")
			textLobPages := table_alloc_pages.FilterByTypeToMap("TEXT")
			table.PageObjectIds = pageObjectIds
			table.setContent(dataPages, lobPages, textLobPages) // correlerate with page object ids

		}

		tables = append(tables, table)
	}

	return tables

}
