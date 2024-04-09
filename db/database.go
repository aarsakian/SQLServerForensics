package db

import (
	LDF "MSSQLParser/ldf"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
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
	ActiveLogRecords    LDF.Records
	CarvedLogRecords    LDF.Records
	tablesInfo          TablesInfo
	columnsinfo         ColumnsInfo
	tablesPartitions    TablesPartitions
	tablesAllocations   TablesAllocations
	columnsPartitions   ColumnsPartitions
	columnsStatistics   ColumnsStatistics
	indexesInfo         IndexesInfo
}

type SystemTable interface {
	Process(page.DataRows)
}

func (db *Database) Process(selectedPage int, fromPage int, toPage int, carve bool) int {

	totalProcessedPages := db.ProcessMDF(selectedPage, fromPage, toPage, carve)
	if db.Lname != "" {
		db.ProcessLDF()
	}

	return totalProcessedPages

}

func (db *Database) ProcessSystemTables() {
	node := db.PagesPerAllocUnitID.GetHeadNode()
	db.tablesInfo = make(TablesInfo)               //objectid -> table info
	db.columnsinfo = make(ColumnsInfo)             //objectid -> column info
	db.tablesPartitions = make(TablesPartitions)   //objectid ->  sysrowsets
	db.tablesAllocations = make(TablesAllocations) //rowsetid -> sysalloc
	db.columnsPartitions = make(ColumnsPartitions) //partitionid ->  sysrscols
	db.indexesInfo = make(IndexesInfo)             //objectid -> index info
	db.columnsStatistics = make(ColumnsStatistics) //objectid ->

	for node != nil {

		for _, page := range node.Pages {
			if page.Header.ObjectId > 100 {
				break
			}
			if page.GetType() != "DATA" {
				continue
			}

			pageType := page.Header.ObjectId

			if pageType == SystemTablesFlags["sysschobjs"] {
				db.tablesInfo.Populate(page.DataRows)

			} else if pageType == SystemTablesFlags["syscolpars"] {
				db.columnsinfo.Populate(page.DataRows)

			} else if pageType == SystemTablesFlags["sysallocationunits"] {
				db.tablesAllocations.Populate(page.DataRows)

			} else if pageType == SystemTablesFlags["sysrscols"] {
				db.columnsPartitions.Populate(page.DataRows)
			} else if pageType == SystemTablesFlags["sysrowsets"] {
				db.tablesPartitions.Populate(page.DataRows)
			} else if pageType == SystemTablesFlags["sysiscols"] {
				db.columnsStatistics.Populate(page.DataRows)
			} else if pageType == SystemTablesFlags["sysidxstats"] {
				db.indexesInfo.Populate(page.DataRows)
			}

			/*	else if pageType == -0x69 { // view object not reached
					var sysobjects *SysObjects = new(SysObjects)
					dataRow.Process(sysobjects)
				} else if pageType == -0x191 { //index_columns
					fmt.Println("INDXE COLS")
				} else if pageType == -0x18d {
					fmt.Println("INDEXES")
				} */

		}

		node = node.Next
	}
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
	recordsProcessed := db.VLFs.Process(*file)
	fmt.Printf("LDF processing completed %d records processed", recordsProcessed)
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

func (db Database) GetName() string {
	return strings.Split(db.Fname, ".")[0]
}

func (db Database) ShowLDF(filterloptype string) {
	for _, vlf := range *db.VLFs {
		vlf.ShowInfo(filterloptype)
	}
}

func (db Database) ShowTables(tablename string, showSchema bool, showContent bool,
	showAllocation string, tabletype string, showrows int, skiprows int,
	showrow int, showcarved bool, showldf bool) {
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
			table.printData(showrows, skiprows, showrow, showcarved, showldf)
			table.cleverPrintData()
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

func (db *Database) AddTablesChangesHistory() {
	var allocatedPages page.Pages

	for idx := range db.Tables {
		var candidateRecords LDF.Records
		for _, allocUnitID := range db.Tables[idx].AllocationUnitIds {
			allocatedPages = db.PagesPerAllocUnitID.GetPages(allocUnitID)
		}

		lop_mod_ins_del_records := db.CarvedLogRecords.FilterByOperations(
			[]string{"LOP_INSERT_ROW", "LOP_DELETE_ROW", "LOP_MODIFY_ROW"})

		lop_mod_ins_del_records = append(lop_mod_ins_del_records,
			db.ActiveLogRecords.FilterByOperations(
				[]string{"LOP_INSERT_ROW", "LOP_DELETE_ROW", "LOP_MODIFY_ROW"})...)

		for _, page := range allocatedPages {
			if page.GetType() != "DATA" {
				continue
			}
			candidateRecords = append(candidateRecords,
				lop_mod_ins_del_records.FilterByPageID(page.Header.PageId)...)
		}

		sort.Sort(LDF.ByDecreasingLSN(candidateRecords))
		db.Tables[idx].addLogChanges(candidateRecords)

	}

}

func (db *Database) GetTables(tablename string) {
	/*
	 get objectid for each table  sysschobjs
	 for each table using its objectid retrieve its columns from syscolpars
	 using the objectid locate the partitions  from sysrowsets
	 using the partitionid locate the allocationunitid  from sysallocationunits

	*/

	for objectid, tableinfo := range db.tablesInfo {
		tname := tableinfo.GetName()
		if tablename != "all" && tablename != tname {
			msg := fmt.Sprintf("table %s not processed", tname)
			mslogger.Mslogger.Info(msg)
			continue
		}

		table := Table{Name: tname, ObjectId: objectid, Type: tableinfo.GetTableType(),
			PageIDsPerType: map[string][]uint32{}}

		msg := fmt.Sprintf("reconstructing table %s  objectId %d type %s", table.Name, table.ObjectId, table.Type)
		mslogger.Mslogger.Info(msg)

		colsinfo := db.columnsinfo[objectid]
		if colsinfo != nil {
			table.addColumns(colsinfo)
			table.sortByColOrder()
		} else {
			msg := fmt.Sprintf("No columns located for table %s", table.Name)
			mslogger.Mslogger.Warning(msg)
		}

		partitions := db.tablesPartitions[objectid]

		var table_alloc_pages page.Pages

		for _, partition := range partitions {
			table.PartitionIds = append(table.PartitionIds, partition.Rowsetid)
			allocationUnits, ok := db.tablesAllocations[partition.Rowsetid] // from sysallocunits PartitionId => page m allocation unit id

			if ok {
				for _, allocationUnit := range allocationUnits {

					table_alloc_pages = append(table_alloc_pages,
						db.PagesPerAllocUnitID.GetPages(allocationUnit.GetId())...) // find the pages the table was allocated
					table.AllocationUnitIds = append(table.AllocationUnitIds,
						allocationUnit.GetId())

				}

				for _, sysrscols := range db.columnsPartitions[partition.Rowsetid] {

					table.updateColOffsets(sysrscols.Rscolid,
						sysrscols.GetLeafOffset()) //columnd_id ,offset
				}

			}

		}

		for _, indexInfo := range db.indexesInfo[objectid] {
			if len(indexInfo.Name) == 0 {
				continue
			}
			allocationUnits, ok := db.tablesAllocations[indexInfo.Rowsetid] // f
			if ok {
				for _, allocationUnit := range allocationUnits {

					table.addIndex(indexInfo, allocationUnit)

				}
			}
			for _, partition := range partitions {
				if partition.Idminor == 0 { // no index
					continue
				}
				for _, sysrscols := range db.columnsPartitions[partition.Rowsetid] {
					if indexInfo.Indid == sysrscols.Hbcolid+1 { // to check normally should be equal
						table.udateColIndex(sysrscols)
						break
					}

				}
			}
		}

		sort.Sort(table_alloc_pages)
		dataPages := table_alloc_pages.FilterByTypeToMap("DATA") // pageId -> Page
		lobPages := table_alloc_pages.FilterByTypeToMap("LOB")
		textLobPages := table_alloc_pages.FilterByTypeToMap("TEXT")
		indexPages := table_alloc_pages.FilterByTypeToMap("Index")
		iamPages := table_alloc_pages.FilterByTypeToMap("IAM")

		table.PageIDsPerType = map[string][]uint32{"DATA": dataPages.GetIDs(), "LOB": lobPages.GetIDs(),
			"Text": textLobPages.GetIDs(), "Index": indexPages.GetIDs(), "IAM": iamPages.GetIDs()}

		if dataPages.IsEmpty() {
			msg := fmt.Sprintf("No pages located for table %s", table.Name)
			mslogger.Mslogger.Warning(msg)

		} else {
			table.setContent(dataPages, lobPages, textLobPages) // correlerate with page object ids

		}

		if !indexPages.IsEmpty() {
			table.setIndexContent(indexPages)
		}

		db.Tables = append(db.Tables, table)
	}

}

func (db Database) DetermineMinLSN(records LDF.Records) utils.LSN {
	lop_end_records := records.FilterByOperation("LOP_END_CKPT")
	latestDate := utils.DateTimeToObj(lop_end_records[0].Lop_End_CKPT.EndTime[:])
	recordId := 0
	for idx, record := range lop_end_records {
		if idx == 0 {
			continue
		}
		newDate := utils.DateTimeToObj(record.Lop_End_CKPT.EndTime[:])
		if newDate.After(latestDate) {
			recordId = idx
			latestDate = newDate
		}
	}
	return lop_end_records[recordId].Lop_End_CKPT.MinLSN
}

func (db Database) FindPageChanges() {
}

func (db *Database) LocateRecords() {
	var records LDF.Records
	for _, vlf := range *db.VLFs {

		for _, block := range vlf.Blocks {
			for _, record := range block.Records {
				records = append(records, record)
			}

		}
	}

	minLSN := db.DetermineMinLSN(records)
	db.ActiveLogRecords = records.FilterByGreaterLSN(minLSN)
	db.CarvedLogRecords = records.FilterByLessLSN(minLSN)

}
