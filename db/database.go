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
	PagesMap page.PagesMap //allocationunitid -> Pages
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

func (db Database) createMap(tablename string) map[any]page.Result[string, string, uint64, uint, uint, uint, uint] {
	results := map[any]page.Result[string, string, uint64, uint, uint, uint, uint]{}
	systemPages := db.PagesMap.FilterBySystemTablesToList(tablename)
	for _, systemPage := range systemPages {
		for _, datarow := range systemPage.DataRows {
			objectId, res := datarow.SystemTable.GetData()
			results[objectId] = res.(page.Result[string, string, uint64, uint, uint, uint, uint])

		}

	}
	return results
}

func (db Database) createMapGeneric(tablename string) map[any]uint64 {
	results := map[any]uint64{}
	systemPages := db.PagesMap.FilterBySystemTablesToList(tablename)
	for _, systemPage := range systemPages {

		for _, datarow := range systemPage.DataRows {
			objectId, val := datarow.SystemTable.GetData()

			results[objectId] = val.(uint64)

		}

	}
	return results
}

func (db Database) createMapListGeneric(tablename string) map[any][]uint64 {
	results := map[any][]uint64{}
	systemPages := db.PagesMap.FilterBySystemTablesToList(tablename)
	for _, systemPage := range systemPages {

		for _, datarow := range systemPage.DataRows {
			objectId, val := datarow.SystemTable.GetData()

			results[objectId] = append(results[objectId], val.(uint64))
		}

	}
	return results
}

func (db Database) createColMapOffsets(tablename string) map[uint64][]page.Result[int32, int16, int64, int32, int32, int16, int32] {
	results := map[uint64][]page.Result[int32, int16, int64, int32, int32, int16, int32]{}
	systemPages := db.PagesMap.FilterBySystemTablesToList(tablename)
	for _, systemPage := range systemPages {
		if systemPage.GetType() != "DATA" {
			continue
		}

		for _, datarow := range systemPage.DataRows {
			partitionId, res := datarow.SystemTable.GetData()

			results[(partitionId).(uint64)] = append(results[(partitionId).(uint64)],
				res.(page.Result[int32, int16, int64, int32, int32, int16, int32]))
		}

	}
	return results
}

func (db Database) createMapListPartitions(tablename string) map[int32][]page.Result[uint64, uint32, uint8, uint16, uint16, uint16, uint32] {
	results := map[int32][]page.Result[uint64, uint32, uint8, uint16, uint16, uint16, uint32]{}
	systemPages := db.PagesMap.FilterBySystemTablesToList(tablename)
	for _, systemPage := range systemPages {
		if systemPage.GetType() != "DATA" {
			continue
		}

		for _, datarow := range systemPage.DataRows {
			objectId, res := datarow.SystemTable.GetData()

			results[(objectId).(int32)] = append(results[(objectId).(int32)],
				res.(page.Result[uint64, uint32, uint8, uint16, uint16, uint16, uint32]))
		}

	}
	return results
}

func (db Database) createMapList(tablename string) map[int32][]page.Result[string, string, int16, uint16, uint32, uint8, uint8] {
	results := map[int32][]page.Result[string, string, int16, uint16, uint32, uint8, uint8]{}
	systemPages := db.PagesMap.FilterBySystemTablesToList(tablename)
	for _, systemPage := range systemPages {

		for _, datarow := range systemPage.DataRows {
			objectId, res := datarow.SystemTable.GetData()

			results[(objectId).(int32)] = append(results[(objectId).(int32)],
				res.(page.Result[string, string, int16, uint16, uint32, uint8, uint8]))
		}

	}
	return results
}

func (db Database) ShowTables(tablename string, showSchema bool, showContent bool,
	showAllocation string, tabletype string, showrows int, showrow int) {
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
			table.printHeader()
			table.printData(showrows, showrow)
		}

		if showAllocation == "simple" {

			table.printAllocation()
		} else if showAllocation == "links" {
			table.printAllocationWithLinks(db.PagesMap)
		}
		tableLocated = true

	}
	if !tableLocated {
		fmt.Printf("Table %s was not found! \n", tablename)
	}

}

func (db Database) GetTablesInformation(tablename string) []Table {
	/*
	 get objectid for each table  sysschobjs
	 for each table using its objectid retrieve its columns from syscolpars
	 using the objectid locate the partitions  from sysrowsets
	 using the partitionid locate the allocationunitid  from sysallocationunits

	*/
	tablesMap := db.createMap("sysschobjs")   // table information holds a map of object ids and table names
	colsMap := db.createMapList("syscolpars") //table objectid = name , type, size, colorder

	colsMapOffsets := db.createColMapOffsets("sysrscols") //Rowsetid = colid ,offset

	tablePartitionsMap := db.createMapListPartitions("sysrowsets")     //(table objectid) = (partitionId, index_id, ...)
	tableSysAllocsMap := db.createMapListGeneric("sysallocationunits") //sysrowsets.Rowsetid =  OwnerId, page allocunitid

	var tables []Table
	for tobjectId, res := range tablesMap {
		tname := res.First

		if tablename != "all" && tablename != tname {
			msg := fmt.Sprintf("table %s not processed", tname)
			mslogger.Mslogger.Info(msg)
			continue
		}

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

		partitions, ok := tablePartitionsMap[tobjectId.(int32)] // from sysrowsets idmajor => rowsetid

		var table_alloc_pages page.Pages

		for _, partition := range partitions {
			table.PartitionIds = append(table.PartitionIds, partition.First)
			allocationUnitIds, ok := tableSysAllocsMap[partition.First] // from sysallocunits PartitionId => page m allocation unit id

			if ok {
				for _, allocationUnitId := range allocationUnitIds {

					table_alloc_pages = append(table_alloc_pages, db.PagesMap[allocationUnitId]...) // find the pages the table was allocated
				}

			}
			table.AllocationUnitIds = allocationUnitIds

			if partition.Second != 1 { // index_id 1 for data pages
				continue
			}
			for _, rscolinfo := range colsMapOffsets[partition.First] {
				table.updateColOffsets(rscolinfo.First, rscolinfo.Second, rscolinfo.Sixth) //columnd_id ,offset, ordkey
			}

		}
		dataPages := table_alloc_pages.FilterByTypeToMap("DATA") // pageId -> Page
		lobPages := table_alloc_pages.FilterByTypeToMap("LOB")
		textLobPages := table_alloc_pages.FilterByTypeToMap("TEXT")
		indexPages := table_alloc_pages.FilterByTypeToMap("Index")
		iamPages := table_alloc_pages.FilterByTypeToMap("IAM")

		table.PageIds = map[string][]uint32{"DATA": utils.Keys(dataPages), "LOB": utils.Keys(lobPages),
			"Text": utils.Keys(textLobPages), "Index": utils.Keys(indexPages), "IAM": utils.Keys(iamPages)}
		table.setContent(dataPages, lobPages, textLobPages) // correlerate with page object ids

		tables = append(tables, table)
	}

	return tables

}
