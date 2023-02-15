package db

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"fmt"
	"sort"
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

func (db Database) createMap(tablename string) map[any]page.Result[string, string, uint64, uint, uint] {
	results := map[any]page.Result[string, string, uint64, uint, uint]{}
	systemPages := db.PagesMap.FilterBySystemTables(tablename)
	for _, tablePages := range systemPages {
		for _, tablePage := range tablePages {
			for _, datarow := range tablePage.DataRows {
				objectId, res := datarow.SystemTable.GetData()
				results[objectId] = res.(page.Result[string, string, uint64, uint, uint])

			}
		}

	}
	return results
}

func (db Database) createMapGeneric(tablename string) map[any]uint64 {
	results := map[any]uint64{}
	systemPages := db.PagesMap.FilterBySystemTables(tablename)
	for _, tablePages := range systemPages {
		for _, tablePage := range tablePages {
			for _, datarow := range tablePage.DataRows {
				objectId, val := datarow.SystemTable.GetData()

				results[objectId] = val.(uint64)

			}
		}

	}
	return results
}

func (db Database) createMapListGeneric(tablename string) map[any][]uint64 {
	results := map[any][]uint64{}
	systemPages := db.PagesMap.FilterBySystemTables(tablename)
	for _, tablePages := range systemPages {
		for _, tablePage := range tablePages {
			for _, datarow := range tablePage.DataRows {
				objectId, val := datarow.SystemTable.GetData()

				results[objectId] = append(results[objectId], val.(uint64))

			}
		}

	}
	return results
}

func (db Database) createMapList(tablename string) map[int32][]page.Result[string, string, int16, uint16, uint32] {
	results := map[int32][]page.Result[string, string, int16, uint16, uint32]{}
	systemPages := db.PagesMap.FilterBySystemTables(tablename)
	for _, tablePages := range systemPages {
		for _, tablePage := range tablePages {
			for _, datarow := range tablePage.DataRows {
				objectId, res := datarow.SystemTable.GetData()

				results[(objectId).(int32)] = append(results[(objectId).(int32)],
					res.(page.Result[string, string, int16, uint16, uint32]))
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
		if tabletype == "user" && table.Type != "User Table" {
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
			/*for _, pageObjecId := range table.PageObjectIds {
				pages := db.PagesMap[pageObjecId]
				for _, page := range pages {
					pageIds[page.Header.PageId] = page.GetType()
				}
			}*/
			table.printAllocation(pageIds)
		}
		tableLocated = true

	}
	if !tableLocated {
		fmt.Printf("Table %s was not found! \n", tablename)
	}

}

func (db Database) GetTablesInformation() []Table {
	/*
	 get objectid for each table  sysschobjs
	 for each table using its objectid retrieve its columns from syscolpars
	 using the objectid locate the partitions  from sysrowsets
	 using the partitionid locate the allocationunitid  from sysallocationunits

	*/
	tablesMap := db.createMap("sysschobjs")   // table information holds a map of object ids and table names
	colsMap := db.createMapList("syscolpars") //table objectid = name , type, size, colorder

	tableAllocsMap := db.createMapListGeneric("sysrowsets")            //(ttable objectid) = sysrowsets.Rowsetid (partitions)
	tableSysAllocsMap := db.createMapListGeneric("sysallocationunits") //sysrowsets.Rowsetid =  OwnerId, page allocunitid

	var tables []Table
	for tobjectId, res := range tablesMap {
		tname := res.First

		results, ok := colsMap[tobjectId.(int32)] // correlate table with its columns

		table := Table{Name: tname, ObjectId: tobjectId.(int32), Type: res.Second, PageIds: map[string][]uint32{}}

		msg := fmt.Sprintf("reconstructing table %s  objectId %d type %s", table.Name, table.ObjectId, table.Type)
		mslogger.Mslogger.Info(msg)

		if ok {
			//		fmt.Printf("Processing table %s with object id %d\n", tname, tobjectId)

			table.addColumns(results)
			table.updateVarLenCols()
			// sort by col order
			sort.Sort(table)

		}

		partitionIds, ok := tableAllocsMap[tobjectId] // from sysrowsets idmajor => rowsetid
		if ok {
			table.PartitionIds = partitionIds // rowsetid
		}
		var table_alloc_pages page.Pages

		for _, partitionId := range partitionIds {
			allocationUnitIds, ok := tableSysAllocsMap[partitionId] // from sysallocunits PartitionId => page m allocation unit id
			if ok {
				for _, allocationUnitId := range allocationUnitIds {

					table_alloc_pages = append(table_alloc_pages, db.PagesMap[allocationUnitId]...) // find the pages the table was allocated
				}

			}
			table.AllocationUnitIds = allocationUnitIds

		}
		dataPages := table_alloc_pages.FilterByTypeToMap("DATA")
		lobPages := table_alloc_pages.FilterByTypeToMap("LOB")
		textLobPages := table_alloc_pages.FilterByTypeToMap("TEXT")

		table.PageIds["Data"] = utils.Keys(dataPages)
		table.PageIds["LOB"] = utils.Keys(lobPages)
		table.PageIds["Text"] = utils.Keys(textLobPages)

		table.setContent(dataPages, lobPages, textLobPages) // correlerate with page object ids

		tables = append(tables, table)
	}

	return tables

}
