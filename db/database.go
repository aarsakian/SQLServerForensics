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
	"sync"
)

var PAGELEN = 8192

type Database struct {
	Fname               string // path to mdf file
	Lname               string // path to ldf file
	Name                string
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
	columnsPartitions   ColumnsPartitions // rowsetid -> sysrscols
	columnsStatistics   ColumnsStatistics // objectid -> sysiscols
	indexesInfo         IndexesInfo
	sysfiles            SysFiles //info about files of db mdf, ldf
}

type SystemTable interface {
	Process(page.DataRows)
}

func (db *Database) ProcessSystemTables() {
	node := db.PagesPerAllocUnitID.GetHeadNode()   //start from head
	db.tablesInfo = make(TablesInfo)               //objectid -> table info
	db.columnsinfo = make(ColumnsInfo)             //objectid -> column info
	db.tablesPartitions = make(TablesPartitions)   //objectid ->  sysrowsets
	db.tablesAllocations = make(TablesAllocations) //rowsetid -> sysalloc
	db.columnsPartitions = make(ColumnsPartitions) //partitionid ->  sysrscols
	db.indexesInfo = make(IndexesInfo)             //objectid -> index info
	db.columnsStatistics = make(ColumnsStatistics) //objectid ->
	db.sysfiles = make(SysFiles, 2)                // mdf, ldf

	for node != nil { //for every alloc unit go over pages

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
			} else if pageType == SystemTablesFlags["sysfiles"] {
				db.sysfiles.Populate(page.DataRows)
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

		allocUnitID := page.Header.GetMetadataAllocUnitId()
		if allocUnitID == 0 {
			msg := fmt.Sprintf("Skipped Processing page at offset %d no alloc unit", offset)
			mslogger.Mslogger.Info(msg)
			continue
		}
		pages.Add(allocUnitID, page)

		totalProcessedPages++

	}
	db.PagesPerAllocUnitID = pages
	return totalProcessedPages
}

func (db *Database) ProcessLDF() (int, error) {
	fmt.Printf("about to process database log file %s \n", db.Lname)

	file, err := os.Open(db.Lname)

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
		fmt.Printf("error reading log page at offset %d\n", offset)
		return 0, err
	}

	db.LogPage = db.ProcessPage(bs, offset, carve)

	db.VLFs = new(LDF.VLFs)
	recordsProcessed := db.VLFs.Process(*file)
	fmt.Printf("LDF processing completed %d log records processed\n", recordsProcessed)

	return recordsProcessed, nil
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

func (db Database) GetTablesInfo() TablesInfo {
	return db.tablesInfo
}

func (db Database) ProcessTables(wg *sync.WaitGroup, tablenames []string, tabletype string, reptables chan<- Table, exptables chan<- Table, tablePages []int) {
	defer wg.Done()
	tablesFound := make(map[string]bool)
	for _, tablename := range tablenames {
		tablesFound[tablename] = false
		for objectid, tableinfo := range db.GetTablesInfo() {

			tname := tableinfo.GetName()

			tableType := tableinfo.GetTableType()

			if tablename != "" && tablename != tname || tabletype != "" && tabletype != tableType {
				msg := fmt.Sprintf("table %s not processed", tname)
				mslogger.Mslogger.Info(msg)
				continue
			}

			table := db.ProcessTable(objectid, tname, tableType, tablePages)
			table.AddChangesHistory(db.PagesPerAllocUnitID, db.CarvedLogRecords, db.ActiveLogRecords)
			exptables <- table
			reptables <- table

			tablesFound[tname] = true

		}
	}

	for tablename, found := range tablesFound {
		if found {
			continue
		}
		fmt.Printf("Table %s not found", tablename)
	}

	close(exptables)
	close(reptables)

}

func (db Database) ProcessTable(objectid int32, tname string, tType string, tablePages []int) Table {
	/*
	 get objectid for each table  sysschobjs
	 for each table using its objectid retrieve its columns from syscolpars
	 using the objectid locate the partitions  from sysrowsets
	 using the partitionid locate the allocationunitid  from sysallocationunits

	*/

	//	fmt.Printf("Processing table %s\n", tname)

	table := Table{Name: tname, ObjectId: objectid, Type: tType,
		PageIDsPerType: map[string][]uint32{}}

	fmt.Printf("reconstructing table %s id %d type %s\n", table.Name, table.ObjectId, table.Type)
	msg := fmt.Sprintf("reconstructing table %s  objectId %d type %s", table.Name, table.ObjectId, table.Type)
	mslogger.Mslogger.Info(msg)

	table.AllocationUnitIdTopartitionId = make(map[uint64]uint64)

	colsinfo := db.columnsinfo[objectid]
	if colsinfo != nil {
		table.addColumns(colsinfo)
		table.sortByColOrder()
	} else {
		msg := fmt.Sprintf("No columns located for table %s", table.Name)
		mslogger.Mslogger.Warning(msg)
	}

	partitions := db.tablesPartitions[objectid] //objectid ->  sysrowsets

	var table_alloc_pages page.Pages

	for _, partition := range partitions {

		allocationUnits, ok := db.tablesAllocations[partition.Rowsetid] // from sysallocunits PartitionId => page m allocation unit id

		if ok {
			for _, allocationUnit := range allocationUnits {

				table_alloc_pages = append(table_alloc_pages,
					db.PagesPerAllocUnitID.GetPages(allocationUnit.GetId())...) // find the pages the table was allocated

				table.AllocationUnitIdTopartitionId[allocationUnit.GetId()] = partition.Rowsetid

			}

			for _, sysrscols := range db.columnsPartitions[partition.Rowsetid] {

				err := table.updateColOffsets(sysrscols.Rscolid,
					sysrscols.GetLeafOffset(), partition.Rowsetid) //columnd_id ,offset
				if err != nil {
					msg := fmt.Sprintf("error in finding column offset rowsetid %d", partition.Rowsetid)
					mslogger.Mslogger.Warning(msg)
				}
			}

		}

	}
	sysiscols := db.columnsStatistics[objectid]
	for _, indexInfo := range db.indexesInfo[objectid] {
		sysallocunits, ok := db.tablesAllocations[indexInfo.Rowsetid]
		table.addIndex(indexInfo, ok, sysallocunits)
	}

	/*
		for _, partition := range partitions {
			if partition.Idminor == 0 { // no index
				continue
			}
			sysrscols := db.columnsPartitions[partition.Rowsetid]
			filteredsSysRsCol := sysrscols.filterByIndexId(indexInfo.Indid)
		}*/

	for _, indexInfo := range db.indexesInfo[objectid] {
		filteredSysIscols := sysiscols.filterByIndexId(indexInfo.Indid)
		table.udateColIndex(filteredSysIscols)
	}

	sort.Sort(table_alloc_pages)
	dataPages := table_alloc_pages.FilterByTypeToMap("DATA") // pageId -> Page
	if tablePages[0] != 0 {
		dataPages = dataPages.FilterByID(tablePages)
	}

	lobPages := table_alloc_pages.FilterByTypeToMap("LOB")
	textLobPages := table_alloc_pages.FilterByTypeToMap("TEXT")
	indexPages := table_alloc_pages.FilterByTypeToMap("Index")
	iamPages := table_alloc_pages.FilterByTypeToMap("IAM")

	table.PageIDsPerType = map[string][]uint32{"DATA": dataPages.GetIDs(), "LOB": lobPages.GetIDs(),
		"Text": textLobPages.GetIDs(), "Index": indexPages.GetIDs(), "IAM": iamPages.GetIDs()}

	if !indexPages.IsEmpty() {
		msg := fmt.Sprintf("No index pages located for table %s", table.Name)
		mslogger.Mslogger.Warning(msg)

	}

	indexedDataPages := table.setIndexContent(indexPages)

	table.PageIDsPerType = map[string][]uint32{"DATA": dataPages.GetIDs(), "LOB": lobPages.GetIDs(),
		"Text": textLobPages.GetIDs(), "Index": indexPages.GetIDs(),
		"IAM": iamPages.GetIDs(), "IndexedDATA": indexedDataPages}
	if len(indexedDataPages) != 0 {
		dataPages = dataPages.FilterByIDSortedByInput(indexedDataPages)
	}

	if dataPages.IsEmpty() {
		msg := fmt.Sprintf("No pages located for table %s", table.Name)
		mslogger.Mslogger.Warning(msg)

	} else {
		table.setContent(dataPages, lobPages, textLobPages) // correlerate with page object ids

	}

	return table

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

func (db *Database) LocateLogRecords() {
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

func (db Database) GetLDFName() string {
	return strings.TrimSpace(db.sysfiles[1].GetName())
}
