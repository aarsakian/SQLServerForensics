package db

import (
	LDF "MSSQLParser/ldf"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"fmt"
	"os"
	"sort"
	"strings"
)

var PAGELEN = 8192

type Database struct {
	Fname               string                  // path to mdf file
	Lname               string                  // path to ldf file
	PagesPerAllocUnitID page.PagesPerId[uint64] //allocationunitid -> Pages
	Tables              []Table
	LogPage             page.Page
	VLFs                *LDF.VLFs
}

func (db *Database) Process(selectedPage int, fromPage int, toPage int, carve bool) int {

	totalProcessedPages := db.ProcessMDF(selectedPage, fromPage, toPage, carve)
	if db.Lname != "" {
		db.ProcessLDF()
	}

	return totalProcessedPages

}

func (db *Database) ProcessMDF(selectedPage int, fromPage int, toPage int, carve bool) int {
	fmt.Printf("about to process database file %s \n", db.Fname)
	file, err := os.Open(db.Fname) //
	if err != nil {
		// handle the error here
		fmt.Printf("err %s reading the mdf file. \n", err)
		fmt.Printf("If you still want to read the mdf file using low level API use -low. This action will copy the file to the temp folder\n")
		fmt.Printf("If you still want to read the mdf use -stopservice to stop sql server running! Please note that that uncommited data migh be lost.\n")
		return -1
	}

	fsize, err := file.Stat() //file descriptor
	if err != nil {
		mslogger.Mslogger.Error(err)
		return -1
	}
	// read the file

	defer file.Close()

	bs := make([]byte, PAGELEN) //byte array to hold one PAGE 8KB

	pages := page.PagesPerId[uint64]{}

	fmt.Println("Processing pages...")
	totalProcessedPages := 0
	for offset := 0; offset < int(fsize.Size()); offset += PAGELEN {
		_, err := file.ReadAt(bs, int64(offset))

		if err != nil {
			fmt.Printf("error reading file --->%s prev offset %d  mod %d",
				err, offset/PAGELEN, offset%PAGELEN)
			break
		}

		if selectedPage != -1 && (offset/PAGELEN < selectedPage ||
			offset/PAGELEN > selectedPage) {
			continue
		}

		if (offset / PAGELEN) < fromPage {
			continue
		}

		if toPage != -1 && (offset/PAGELEN) > toPage {
			continue
		}
		msg := fmt.Sprintf("Processing offset %d", offset)
		mslogger.Mslogger.Info(msg)
		page := db.ProcessPage(bs, offset, carve)
		pages.Add(page.Header.GetMetadataAllocUnitId(), page)

		totalProcessedPages++

	}
	db.PagesPerAllocUnitID = pages
	return totalProcessedPages
}

func (db *Database) ProcessLDF() {
	fmt.Printf("about to process database log file %s \n", db.Lname)
	file, err := os.Open(db.Lname) //

	if err != nil {
		// handle the error here
		fmt.Printf("err %s reading the ldf file. \n", err)
	}
	defer file.Close()
	offset := 0
	carve := false
	bs := make([]byte, PAGELEN) //byte array to hold one PAGE 8KB
	_, err = file.ReadAt(bs, int64(offset))
	if err != nil {
		fmt.Printf("error reading log page ---\n")
		return
	}

	db.LogPage = db.ProcessPage(bs, offset, carve)

	db.VLFs = new(LDF.VLFs)
	db.VLFs.Process(*file)
}

func (db Database) ProcessPage(bs []byte, offset int, carve bool) page.Page {
	var page *page.Page = new(page.Page)
	page.Process(bs, offset, carve)

	return *page
}

func (db *Database) FilterBySystemTables(systemTables string) {
	db.PagesPerAllocUnitID = db.PagesPerAllocUnitID.FilterBySystemTables(systemTables)
}

func (db *Database) FilterPagesByType(pageType string) {
	db.PagesPerAllocUnitID = db.PagesPerAllocUnitID.FilterByType(pageType) //mutable
}

func (db *Database) FilterPagesBySystemTables(systemTable string) {
	db.PagesPerAllocUnitID = db.PagesPerAllocUnitID.FilterBySystemTables(systemTable)
}

func (db Database) createMap(tablename string) map[any]page.Result[string, string, uint64, uint, uint, uint, uint] {
	results := map[any]page.Result[string, string, uint64, uint, uint, uint, uint]{}
	systemPages := db.PagesPerAllocUnitID.FilterBySystemTablesToList(tablename)
	for _, systemPage := range systemPages {
		for _, datarow := range systemPage.DataRows {
			objectId, res := datarow.SystemTable.GetData()
			results[objectId] = res.(page.Result[string, string, uint64, uint, uint, uint, uint])

		}

	}
	return results
}

func (db Database) GetName() string {
	return strings.Split(db.Fname, ".")[0]
}

func (db Database) createMapGeneric(tablename string) map[any]uint64 {
	results := map[any]uint64{}
	systemPages := db.PagesPerAllocUnitID.FilterBySystemTablesToList(tablename)
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
	systemPages := db.PagesPerAllocUnitID.FilterBySystemTablesToList(tablename)
	for _, systemPage := range systemPages {

		for _, datarow := range systemPage.DataRows {
			objectId, val := datarow.SystemTable.GetData()

			results[objectId] = append(results[objectId], val.(uint64))
		}

	}
	return results
}

func (db Database) createColMapListOffsets(tablename string) map[uint64][]page.Result[int32, int16, int64, int32, int32, int16, int32] {
	results := map[uint64][]page.Result[int32, int16, int64, int32, int32, int16, int32]{}
	systemPages := db.PagesPerAllocUnitID.FilterBySystemTablesToList(tablename)
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
	systemPages := db.PagesPerAllocUnitID.FilterBySystemTablesToList(tablename)
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
	systemPages := db.PagesPerAllocUnitID.FilterBySystemTablesToList(tablename)
	for _, systemPage := range systemPages {

		for _, datarow := range systemPage.DataRows {
			objectId, res := datarow.SystemTable.GetData()

			results[(objectId).(int32)] = append(results[(objectId).(int32)],
				res.(page.Result[string, string, int16, uint16, uint32, uint8, uint8]))
		}

	}
	return results
}

func (db Database) ShowLDF(filterloptype string) {
	for _, vlf := range *db.VLFs {
		vlf.ShowInfo(filterloptype)
	}
}

func (db Database) ShowTables(tablename string, showSchema bool, showContent bool,
	showAllocation string, tabletype string, showrows int, showrow int, showcarved bool) {
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
			table.printData(showrows, showrow, showcarved)
		}

		if showAllocation == "simple" {

			table.printAllocation()
		} else if showAllocation == "links" {
			table.printAllocationWithLinks()
		}
		tableLocated = true

	}
	if !tableLocated {
		fmt.Printf("Table %s was not found! \n", tablename)
	}

}

func (db *Database) GetTables(tablename string) {
	/*
	 get objectid for each table  sysschobjs
	 for each table using its objectid retrieve its columns from syscolpars
	 using the objectid locate the partitions  from sysrowsets
	 using the partitionid locate the allocationunitid  from sysallocationunits

	*/
	tablesMap := db.createMap("sysschobjs")   // table objectid = table info
	colsMap := db.createMapList("syscolpars") //table objectid =[] name , type, size, colorder

	colsMapOffsets := db.createColMapListOffsets("sysrscols") //Rowsetid =  []colid ,offset

	tablePartitionsMap := db.createMapListPartitions("sysrowsets")     //(table objectid) = [](partitionId, index_id, ...)
	tableSysAllocsMap := db.createMapListGeneric("sysallocationunits") //sysrowsets.Rowsetid =  []OwnerId, page allocunitid

	for tobjectId, res := range tablesMap {
		tname := res.First

		if tablename != "all" && tablename != tname {
			msg := fmt.Sprintf("table %s not processed", tname)
			mslogger.Mslogger.Info(msg)
			continue
		}
		table := Table{Name: tname, ObjectId: tobjectId.(int32), Type: res.Second, PageIds: map[string][]uint32{}}
		msg := fmt.Sprintf("reconstructing table %s  objectId %d type %s", table.Name, table.ObjectId, table.Type)
		mslogger.Mslogger.Info(msg)

		results, ok := colsMap[tobjectId.(int32)] // correlate table with its columns

		if ok {
			table.addColumns(results)
			table.sortByColOrder()
		} else {
			msg := fmt.Sprintf("No columns located for table %s", table.Name)
			mslogger.Mslogger.Warning(msg)
		}

		partitions := tablePartitionsMap[tobjectId.(int32)] // from sysrowsets idmajor => rowsetid

		var table_alloc_pages page.Pages

		for _, partition := range partitions {
			table.PartitionIds = append(table.PartitionIds, partition.First)
			allocationUnitIds, ok := tableSysAllocsMap[partition.First] // from sysallocunits PartitionId => page m allocation unit id

			if ok {
				for _, allocationUnitId := range allocationUnitIds {

					table_alloc_pages = append(table_alloc_pages, db.PagesPerAllocUnitID.GetPages(allocationUnitId)...) // find the pages the table was allocated
				}

			}
			table.AllocationUnitIds = allocationUnitIds

			if partition.Second != 1 { // index_id 1 for data pages
				msg := fmt.Sprintf("Table %s has partition heap index id %d\n",
					table.Name, partition.Second)
				mslogger.Mslogger.Info(msg)
			}
			for _, rscolinfo := range colsMapOffsets[partition.First] {
				table.updateColOffsets(rscolinfo.First, rscolinfo.Second, rscolinfo.Sixth) //columnd_id ,offset, ordkey
			}

		}

		sort.Sort(table_alloc_pages)
		dataPages := table_alloc_pages.FilterByTypeToMap("DATA") // pageId -> Page
		lobPages := table_alloc_pages.FilterByTypeToMap("LOB")
		textLobPages := table_alloc_pages.FilterByTypeToMap("TEXT")
		indexPages := table_alloc_pages.FilterByTypeToMap("Index")
		iamPages := table_alloc_pages.FilterByTypeToMap("IAM")

		table.PageIds = map[string][]uint32{"DATA": dataPages.GetIDs(), "LOB": lobPages.GetIDs(),
			"Text": textLobPages.GetIDs(), "Index": indexPages.GetIDs(), "IAM": iamPages.GetIDs()}

		if dataPages.IsEmpty() {
			msg := fmt.Sprintf("No pages located for table %s", table.Name)
			mslogger.Mslogger.Warning(msg)

		} else {
			table.setContent(dataPages, lobPages, textLobPages) // correlerate with page object ids

		}

		db.Tables = append(db.Tables, table)
	}

}
