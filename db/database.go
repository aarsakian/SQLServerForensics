package db

import (
	"MSSQLParser/data"
	tables "MSSQLParser/db/tables"
	LDF "MSSQLParser/ldf"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

var PAGELEN = 8192

type Database struct {
	BakName string // path to bak payload file
	Fname   string // path to mdf file
	Lname   string // path to ldf file

	Name                string
	NofPages            int
	BindingID           [16]byte
	PagesPerAllocUnitID page.PagesPerId[uint64] //allocationunitid -> Pages
	Tables              []Table
	LogDB               *LogDB
	tablesInfo          tables.TablesInfo
	columnsinfo         tables.ColumnsInfo
	tablesPartitions    tables.TablesPartitions
	tablesAllocations   tables.TablesAllocations
	columnsPartitions   tables.ColumnsPartitions // rowsetid -> sysrscols
	columnsStatistics   tables.ColumnsStatistics // objectid -> sysiscols
	metadataBlobs       tables.MetadataBlobs
	indexesInfo         tables.IndexesInfo
	sysfiles            tables.SysFiles //info about files of db mdf, ldf
	DbiCheckptLSN       utils.LSN
	MinLSN              utils.LSN
	DirtyPages          map[uint32]utils.LSN
	State               string
}

type SystemTable interface {
	Process(data.DataRows)
}

func (db *Database) ProcessSystemTables() {
	node := db.PagesPerAllocUnitID.GetHeadNode()          //start from head
	db.tablesInfo = make(tables.TablesInfo)               //objectid -> table info
	db.columnsinfo = make(tables.ColumnsInfo)             //objectid -> column info
	db.tablesPartitions = make(tables.TablesPartitions)   //objectid ->  sysrowsets
	db.tablesAllocations = make(tables.TablesAllocations) //rowsetid -> sysalloc
	db.columnsPartitions = make(tables.ColumnsPartitions) //partitionid ->  sysrscols
	db.indexesInfo = make(tables.IndexesInfo)             //objectid -> index info
	db.columnsStatistics = make(tables.ColumnsStatistics) //objectid ->
	db.sysfiles = make(tables.SysFiles, 2)                // mdf, ldf
	db.metadataBlobs = make(tables.MetadataBlobs)         //objectid -> metadata blobs

	for node != nil { //for every alloc unit go over pages

		for _, page := range node.Pages {

			if page.Header.ObjectId > 100 {
				break
			}
			if page.GetType() != "DATA" {
				continue
			}

			pageType := page.Header.ObjectId

			switch pageType {
			case tables.SystemTablesFlags["sysschobjs"]:
				db.tablesInfo.Populate(page.DataRows)
			case tables.SystemTablesFlags["syscolpars"]:
				db.columnsinfo.Populate(page.DataRows)
			case tables.SystemTablesFlags["sysallocationunits"]:
				db.tablesAllocations.Populate(page.DataRows)
			case tables.SystemTablesFlags["sysrscols"]:
				db.columnsPartitions.Populate(page.DataRows)
			case tables.SystemTablesFlags["sysrowsets"]:
				db.tablesPartitions.Populate(page.DataRows)
			case tables.SystemTablesFlags["sysiscols"]:
				db.columnsStatistics.Populate(page.DataRows)
			case tables.SystemTablesFlags["sysidxstats"]:
				db.indexesInfo.Populate(page.DataRows)
			case tables.SystemTablesFlags["sysfiles"]:
				db.sysfiles.Populate(page.DataRows)
			case tables.SystemTablesFlags["sysobjvalues"]:
				db.metadataBlobs.Populate(page.DataRows)
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

	msg := fmt.Sprintf("Processed system tables of %s .", db.Name)
	mslogger.Mslogger.Info(msg)
	fmt.Printf("msg %s\n", msg)
}

func (db *Database) ProcessBAK(carve bool) (int, error) {
	file, err := os.Open(db.BakName) //
	if err != nil {
		// handle the error here
		fmt.Printf("err %s reading the bak file. \n", err)
		return 0, err

	}
	defer file.Close()

	processedPage, _, err := db.ProcessPages(file, []int{}, -1, math.MaxInt, carve)

	return processedPage, err

}

func (db *Database) ProcessMDF(selectedPages []int, fromPage int, toPage int, carve bool) (int, error) {
	var processesPages int

	file, err := os.Open(db.Fname) //
	if err != nil {
		// handle the error here
		fmt.Printf("err %s reading the mdf file. \n", err)

		fmt.Printf(`If you still want to read the mdf file using low level API use -low.
		 This action will logically copy the clusters of the file to the temp folder\n`)

		fmt.Printf(`If you still want to read the mdf use -stopservice to stop sql server running!
		 Please note that that uncommited data migh be lost.\n`)
		return 0, err
	}
	defer file.Close()
	fmt.Printf("Processing mdf file %s\n", db.Fname)
	processesPages, _, err = db.ProcessPages(file, selectedPages, fromPage, toPage, carve)
	return processesPages, err
}

func (db *Database) ProcessPages(file *os.File, selectedPages []int, fromPage int, toPage int, carve bool) (int, []int, error) {
	totalProcessedPages := 0
	//offsets that might contain log records and other useful info
	var suspisiousOffsets []int
	selectedPagesMap := make(map[int]bool)
	for _, selectedPage := range selectedPages {
		selectedPagesMap[selectedPage] = true
	}

	fsize, err := file.Stat() //file descriptor
	if err != nil {
		mslogger.Mslogger.Error(err)
		return 0, suspisiousOffsets, err
	}
	// read the file

	bs := make([]byte, PAGELEN) //byte array to hold one PAGE 8KB

	pages := page.PagesPerId[uint64]{}

	for offset := 0; offset < int(fsize.Size()); offset += PAGELEN {
		_, err := file.ReadAt(bs, int64(offset))

		if err != nil {
			fmt.Printf("error reading file --->%s prev offset %d  mod %d",
				err, offset/PAGELEN, offset%PAGELEN)
			return 0, suspisiousOffsets, err
		}

		if len(selectedPages) > 0 && !selectedPagesMap[offset/PAGELEN] {

			continue
		}

		if (offset / PAGELEN) < fromPage {
			continue
		}

		if toPage != -1 && (offset/PAGELEN) > toPage {
			continue
		}
		msg := fmt.Sprintf("Processing page at %d", offset)
		mslogger.Mslogger.Info(msg)
		page_, err := db.ProcessPage(bs, offset, carve, db.NofPages)

		switch err.(type) {
		case page.InvalidPageTypeError:
			suspisiousOffsets = append(suspisiousOffsets, offset)
		}

		if page_.FileHeader != nil {
			db.NofPages = int(page_.FileHeader.Size)
			db.BindingID = page_.FileHeader.BindingID
		}

		if db.Name == "" && page_.Boot != nil {
			db.Name = page_.Boot.GetDBName()
			//the minimum LSN of flushed pages (dirty)
			db.DbiCheckptLSN = page_.Boot.Dbi_checkptLSN
		}

		allocUnitID := page_.Header.GetMetadataAllocUnitId()
		if allocUnitID == 0 {
			msg := fmt.Sprintf("Skipped Processing page at offset %d no alloc unit", offset)
			mslogger.Mslogger.Info(msg)
			continue
		}
		pages.Add(allocUnitID, page_)

		totalProcessedPages++

	}
	msg := fmt.Sprintf("Processing pages of %s completed ", db.Name)
	mslogger.Mslogger.Info(msg)
	fmt.Printf("%s\n", msg)

	db.PagesPerAllocUnitID = pages
	return totalProcessedPages, suspisiousOffsets, nil
}

func (db Database) ProcessPage(bs []byte, offset int, carve bool, nofpages int) (page.Page, error) {
	page := new(page.Page)
	err := page.Process(bs, offset, carve, nofpages)

	return *page, err
}

func (db *Database) FilterBySystemTables(systemTables string) {
	db.PagesPerAllocUnitID = db.PagesPerAllocUnitID.FilterBySystemTables(systemTables)
}

func (db *Database) FilterPagesByTypeMutable(pageType string) {
	db.PagesPerAllocUnitID = db.PagesPerAllocUnitID.FilterByType(pageType) //mutable
}

func (db *Database) FilterPagesByType(pageType string) page.PagesPerId[uint64] {
	return db.PagesPerAllocUnitID.FilterByType(pageType)
}

func (db *Database) FilterPagesBySystemTables(systemTable string) {
	db.PagesPerAllocUnitID = db.PagesPerAllocUnitID.FilterBySystemTables(systemTable)
}

func (db Database) GetTablesInfo() tables.TablesInfo {
	return db.tablesInfo
}

func (db Database) ProcessTables(ctx context.Context, tablenames []string, tabletype string,
	tablesCH chan<- Table, tablePages []int) {

	defer close(tablesCH)

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
			fmt.Printf("Processing Table %s\n", tname)

			table := db.ProcessTable(objectid, tname, tableType, tablePages)
			if db.LogDB != nil {
				table.AddChangesHistory(db.PagesPerAllocUnitID, db.LogDB.LogRecordsMap)
			}

			select {
			case tablesCH <- table:
				tablesFound[tname] = true
			case <-ctx.Done():
				return
			}

		}
	}

	for tablename, found := range tablesFound {
		if !found {
			fmt.Printf("Table %s not found\n", tablename)
		}

	}

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

	msg := fmt.Sprintf("reconstructing table %s  objectId %d type %s", table.Name, table.ObjectId, table.Type)
	mslogger.Mslogger.Info(msg)

	table.AllocationUnitIdTopartitionId = make(map[uint64]uint64)

	colsinfo := db.columnsinfo[objectid]

	if colsinfo != nil {

		table.addColumns(colsinfo)
		table.sortByColOrder()
		table.setVarLenCols()

	} else {
		msg := fmt.Sprintf("No columns located for table %s", table.Name)
		mslogger.Mslogger.Warning(msg)
	}

	metadataBlobsInfo, ok := db.metadataBlobs[objectid]
	if ok {
		table.setMetadataBlobs(metadataBlobsInfo)
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
		filteredSysIscols := sysiscols.FilterByIndexId(indexInfo.Indid)
		table.udateColIndex(filteredSysIscols)
	}

	sort.Sort(table_alloc_pages)
	dataPages := table_alloc_pages.FilterByTypeToMap("DATA") // pageId -> Page
	if len(tablePages) > 0 {
		dataPages = dataPages.FilterByID(tablePages)
	}

	lobPages := table_alloc_pages.FilterByTypeToMap("LOB")
	textLobPages := table_alloc_pages.FilterByTypeToMap("TEXT")
	indexPages := table_alloc_pages.FilterByTypeToMap("Index")
	iamPages := table_alloc_pages.FilterByTypeToMap("IAM")

	table.PageIDsPerType = map[string][]uint32{"DATA": dataPages.GetIDs(), "LOB": lobPages.GetIDs(),
		"Text": textLobPages.GetIDs(), "Index": indexPages.GetIDs(), "IAM": iamPages.GetIDs()}

	if indexPages.IsEmpty() {
		msg := fmt.Sprintf("No index pages located for table %s", table.Name)
		mslogger.Mslogger.Warning(msg)

	}

	indexedDataPerPage := table.setIndexContent(indexPages)

	table.PageIDsPerType = map[string][]uint32{"DATA": dataPages.GetIDs(), "LOB": lobPages.GetIDs(),
		"Text": textLobPages.GetIDs(), "Index": indexPages.GetIDs(),
		"IAM": iamPages.GetIDs(), "IndexedDATA": indexedDataPerPage}

	if len(indexedDataPerPage) != 0 {
		//this is to keep the order by indexed column value
		//dataPages = dataPages.FilterByIDSortedByInput(indexedDataPages)
	}

	if dataPages.IsEmpty() {
		msg := fmt.Sprintf("No pages located for table %s", table.Name)
		mslogger.Mslogger.Warning(msg)

	} else {
		table.setContent(dataPages, lobPages, textLobPages) // correlerate with page object ids
		msg := fmt.Sprintf("Processing %s completed, rows: %d", table.Name, len(table.Rows))
		mslogger.Mslogger.Info(msg)
		fmt.Printf("%s\n", msg)
	}

	return table

}

func (db *Database) GetLogRecords() LDF.Records {
	return db.LogDB.GetRecords()
}

func (db *Database) UpdateState(minLsn utils.LSN) {
	db.MinLSN = minLsn
	if minLsn.IsLess(db.DbiCheckptLSN) {
		fmt.Printf("Database has open transactions that started after the checkpoint began minLSN %s checkpoint begin LSN %s\n",
			minLsn.ToStr(), db.DbiCheckptLSN.ToStr())

		db.State = "dirty"

	}

}

func (db *Database) AddLogRecords() {

	db.LogDB.LogRecordsMap = make(LDF.RecordsMap)

	//cross validate with records

	for _, record := range db.LogDB.GetRecords() {
		record.UpdateActiveStatus(db.MinLSN)
		db.LogDB.LogRecordsMap[record.CurrentLSN] = record
	}

}

func (db *Database) LocateDirtyPages() {
	db.DirtyPages = db.LogDB.LocateDirtyPages(db.DbiCheckptLSN)

}

func (db Database) GetLDFName() string {
	return strings.TrimSpace(db.sysfiles[1].GetName())
}

func (db Database) CorrelateLDFToPages() {
	//correlate using LSN
	node := db.PagesPerAllocUnitID.GetHeadNode()
	for node != nil {
		for idx := range node.Pages {
			record, ok := db.LogDB.LogRecordsMap[node.Pages[idx].Header.LSN]
			if ok {
				node.Pages[idx].LDFRecord = record
				break
			}
		}
		node = node.Next
	}

}

func (db Database) GetBindingID() string {
	return utils.StringifyGUID(db.BindingID[:])
}
