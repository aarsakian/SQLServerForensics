package db

import (
	"MSSQLParser/correlation"
	"MSSQLParser/data"
	tables "MSSQLParser/db/tables"
	LDF "MSSQLParser/ldf"
	"MSSQLParser/logger"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"sync"
)

type Table struct {
	Name                           string
	ObjectId                       int32
	Type                           string
	Rows                           []*tables.Row
	AllocationUnitIdTopartitionId  map[uint64]uint64
	PartitionIdToAllocationsUnitId map[uint64][]uint64
	Schema                         []tables.Column
	Indexes                        []tables.Index
	VarLenCols                     []int
	PageIDsPerType                 map[string][]uint32 //pageType ->pageID
	indexType                      string
	logRecords                     LDF.Records
}

type ByRowId []tables.ColMap

type ByColOrder []tables.Column

func (b ByColOrder) Less(i, j int) bool {

	return b[i].Order < b[j].Order
}

func (b ByColOrder) Swap(i, j int) {

	b[i], b[j] = b[j], b[i]
}

func (b ByColOrder) Len() int {
	return len(b)

}

type ByActionDate []tables.Row

func (row ByActionDate) Less(i, j int) bool {

	return row[i].LogDate.Before(row[j].LogDate)
}

func (row ByActionDate) Swap(i, j int) {

	row[i], row[j] = row[j], row[i]
}

func (row ByActionDate) Len() int {
	return len(row)

}

func (table Table) sortByColOrder() {
	// sort by col order
	sort.Sort(ByColOrder(table.Schema))
}

type OrderedRows []tables.Row

/*func (byrowid ByRowId) Len() int {
	return len(byrowid)

}

func (byrowid ByRowId) Less(i, j int) bool {
	return byrowid[i] < byrowid[j]
}

func (byrowid ByRowId) Swap(i, j int) {

	byrowid[i], byrowid[j] = byrowid[j], byrowid[i]
}*/

func (table *Table) udateColIndex(sysiscols tables.SysIsCols) {
	for _, sysiscol := range sysiscols {
		for indexIdx, indexInfo := range table.Indexes {
			if indexInfo.Id != sysiscol.Idminor {
				continue
			}
			for idx, col := range table.Schema {
				if col.Order != uint16(sysiscol.Intprop) {
					continue

				}
				table.Indexes[indexIdx].Columns = append(table.Indexes[indexIdx].Columns,
					&table.Schema[idx])

			}

		}

	}
}

func (table *Table) AddChangesHistory(correlatedRecords []*correlation.CorrelatedRecord,
	pagesPerAllocUnitID page.PagesPerId[uint64]) {

	//flag denotes carved
	//	table.addLogChanges(candidateRecords)

	partitionIDToMap := make(map[uint64]*page.Page)

	for _, record := range correlatedRecords {

		allocunitids := table.PartitionIdToAllocationsUnitId[record.PartitionId]
		for _, alloc := range allocunitids {
			pagesPerIdNode := pagesPerAllocUnitID.Lookup[alloc]
			if pagesPerIdNode == nil {
				msg := fmt.Sprintf("Allocation unit id %d not found in pages per allocation unit id for partition id %d and record LSN %s",
					alloc, record.PartitionId, record.LSN.ToStr())
				mslogger.Mslogger.Warning(msg)
				continue
			}
			page := pagesPerIdNode.PagesMap[record.RowId.PageId]
			if page == nil {
				continue
			}
			partitionIDToMap[record.PartitionId+uint64(record.RowId.PageId)] = page
		}
	}

	for _, row := range table.Rows {

		for _, record := range correlatedRecords {
			// correlateRecord found
			if record.RowId != nil && row.RowId.IsEqual(*record.RowId) {
				page, ok := partitionIDToMap[record.PartitionId+uint64(row.RowId.PageId)]
				//page has already been flashed record exists
				if !ok {
					continue
				}
				row.UpdateWithLogRecord(record.LogRecord,
					page.Header.LSN.IsGreaterEqual(record.LogRecord.CurrentLSN), table.Schema)

			}
		}
	}
	//add records to table not existing

	for _, pageId := range table.PageIDsPerType["DATA"] {
		for _, record := range correlatedRecords {
			// correlateRecord found
			if record.RowId != nil {
				page, ok := partitionIDToMap[record.PartitionId+uint64(pageId)]
				//page has already been flashed record exists
				if !ok {
					continue
				}
				table.AddLogBasedRecord(record.LogRecord,
					page.Header.LSN.IsGreaterEqual(record.LogRecord.CurrentLSN))

			}
		}

	}

	//table.logRecords = candidateRecords

}

func (table *Table) AddLogBasedRecord(record *LDF.Record, pageFlushed bool) {
	// && !slotRecordsPerGroup.HasExpungeOperation(idx)
	if record.GetOperationType() == "LOP_DELETE_ROW" && pageFlushed ||
		record.GetOperationType() == "LOP_INSERT_ROW" && !pageFlushed {

		table.AddRow(record, true)

		//} else if record.GetOperationType() == "LOP_DELETE_ROW" && slotRecordsPerGroup.HasExpungeOperation(idx) {
		//		table.AddPurgedRow(*record, record.Carved)

	}

}

func (table *Table) addIndex(indexInfo tables.SysIdxStats, hasallocunits bool, sysallocunits []tables.SysAllocUnits) {

	tableIndex := tables.Index{Id: indexInfo.Indid, Name: indexInfo.GetName(), IsClustered: indexInfo.Type == 1}

	if hasallocunits {
		for _, sysallocunit := range sysallocunits {
			tableIndex.AddAllocatedPages(sysallocunit)

		}
	}

	table.Indexes = append(table.Indexes, tableIndex)

}

func (table *Table) setIndexContent(indexPages page.PagesPerId[uint32]) []uint32 {
	var indexedDataPages []uint32
	for idx := range table.Indexes {
		if !table.Indexes[idx].IsClustered {
			continue
		}

		indexedDataPages = table.Indexes[idx].Populate(indexPages)

	}

	return indexedDataPages

	/*for _, indexrow := range page.IndexRows {

		if indexrow.NoNLeaf == nil {
			continue
		}

		data := indexrow.NoNLeaf.KeyValue

			if tindex.IsClustered && int(c.Size) != len(indexrow.NoNLeaf.KeyValue)-4 && //4 bytes to ensure uniqueness of cluster key
				int(c.Size) != len(indexrow.NoNLeaf.KeyValue) {
				break
			}
			keystr := c.toString(indexrow.NoNLeaf.KeyValue)
			if keystr == "0" { //?
				break
			}

		for rowid, row := range table.Rows {
			startOffset := 0
			located := false
			for _, c := range tindex.columns {
				//must match every column in the index
				if c.toString(row.ColMap[c.Name].Content) !=
					c.toString(data[startOffset:startOffset+int(c.Size)]) {

					located = false
					break
				}
				located = true
				startOffset += int(c.Size)
			}
			if located {
				table.orderedRows = append(table.orderedRows, &table.Rows[rowid])
			}

		}

	}

	*/

}

func (table *Table) AddRow(record *LDF.Record, carved bool) {

	lobPages := page.PagesPerId[uint32]{}
	textLobPages := page.PagesPerId[uint32]{}
	colMap := make(tables.ColMap)
	nofNullCols := 0
	for _, col := range table.Schema {
		if record.Lop_Insert_Delete.DataRow == nil {
			lsn := record.CurrentLSN.ToStr()
			msg := fmt.Sprintf("Lop Insert Record missing DataRow %s", lsn)
			mslogger.Mslogger.Warning(msg)
			continue
		}

		colval, e := col.AddContent(*record.Lop_Insert_Delete.DataRow, lobPages, textLobPages, record.Lop_Insert_Delete.PartitionID, nofNullCols)
		if e == nil {
			colMap[col.Name] = tables.ColData{Content: colval}
		}

	}

	loggedOperation := "Inserted at "
	loggedOperation += record.GetBeginCommitDate()
	loggedOperation += fmt.Sprintf(" commited at %s", record.GetEndCommitDate())

	table.Rows = append(table.Rows, &tables.Row{ColMap: colMap, LoggedOperation: loggedOperation,
		LogDate: record.GetBeginCommitDateObj(), Carved: carved, Logged: true})
}

func (table *Table) AddPurgedRow(record LDF.Record, carved bool) error {
	foundRowMatch := false

	loggedOperation := "Deleted at " + record.GetBeginCommitDate() +
		fmt.Sprintf(" commited at %s previous slot %d", record.GetEndCommitDate(),
			record.Lop_Insert_Delete.RowId.SlotNumber)
	if record.Lop_Insert_Delete.DataRow == nil {
		msg := fmt.Sprintf("Table %s and record LSN %s with LOP_INSERT_DELETE has no datarow",
			table.Name, record.CurrentLSN.ToStr())
		mslogger.Mslogger.Warning(msg)
		return errors.New(msg)

	}
	row := table.ProcessRow(*record.Lop_Insert_Delete.DataRow,
		page.PagesPerId[uint32]{}, page.PagesPerId[uint32]{}, record.Lop_Insert_Delete.PartitionID)

	//before adding a purged row check if the same row was carved
	for rowid, existingRow := range table.Rows {
		if !existingRow.Carved {
			continue
		}
		if reflect.DeepEqual(existingRow.ColMap, row.ColMap) {
			existingRow.Carved = carved
			existingRow.Logged = true
			existingRow.LoggedOperation = loggedOperation
			existingRow.LogDate = record.GetBeginCommitDateObj()
			table.Rows[rowid] = existingRow

			foundRowMatch = true
		}
	}

	if !foundRowMatch {

		row.Carved = carved
		row.Logged = true
		row.LoggedOperation = loggedOperation
		row.LogDate = record.GetBeginCommitDateObj()
		table.Rows = append(table.Rows, &row)
	}

	return nil
}

func (table *Table) addColumn(column tables.Column) {

	table.Schema = append(table.Schema, column)

}

func (table *Table) setVarLenCols() {

	vid := 0
	for idx := range table.Schema {
		if table.Schema[idx].IsStatic() {
			continue
		}
		if table.Schema[idx].IsComputed {
			continue
		}
		table.Schema[idx].VarLenOrder = uint16(vid)

		vid++
	}
}

func (table *Table) setMetadataBlobs(sysobjvalues []tables.SysObjValues) {

	for _, sysobjvalue := range sysobjvalues {
		// metadata fragment
		if sysobjvalue.Valclass == 2 && int(sysobjvalue.Subobjid-1) < len(table.Schema) {

			table.Schema[sysobjvalue.Subobjid-1].Computed =
				&tables.Computed{Definition: string(sysobjvalue.Imageval[:])}
		}

	}

}

func (table *Table) addColumns(columns []tables.SysColpars) {

	for _, col := range columns {

		codepage, _, err := utils.LocateEncoding(fmt.Sprintf("%d", col.Collationid))

		if err == nil {

			table.addColumn(tables.Column{Name: col.GetName(), Type: col.GetType(),
				Size: col.Length, Order: col.Colid, CollationId: col.Collationid,
				Charmap:   utils.LocateWindowsCharmap(codepage),
				CodePage:  codepage,
				Precision: col.Prec, Scale: col.Scale,
				OffsetMap:    map[uint64]int16{},
				IsAnsiPadded: col.IsAnsiPadded(),
				IsIdentity:   col.IsIdentity(),
				IsRowGUIDCol: col.IsRowGUIDCol(),
				IsComputed:   col.IsComputed(),
				IsColumnSet:  col.IsColumnSet(),
				IsFilestream: col.IsFilestream(),
				IsPersisted:  col.IsPersisted(),
			})

		} else {
			table.addColumn(tables.Column{Name: col.GetName(), Type: col.GetType(),
				Size: col.Length, Order: col.Colid, CollationId: col.Collationid,
				Precision: col.Prec, Scale: col.Scale,
				OffsetMap: map[uint64]int16{}, IsAnsiPadded: col.IsAnsiPadded(),
				IsIdentity: col.IsIdentity(), IsRowGUIDCol: col.IsRowGUIDCol(),
				IsComputed: col.IsComputed(), IsColumnSet: col.IsColumnSet(),
				IsFilestream: col.IsFilestream(), IsPersisted: col.IsPersisted()})
		}

	}

}

func (table Table) printSchema() {
	if table.Schema != nil {

		fmt.Printf("Static cols \n")
		for _, col := range table.Schema {
			if !col.IsStatic() {
				continue
			}
			fmt.Printf(" | %s %s Padded %t Identity %t RowGUID %t Computed %t Filestream %t",
				col.Name, col.Type, col.IsAnsiPadded, col.IsIdentity, col.IsRowGUIDCol, col.IsComputed, col.IsFilestream)
		}
		fmt.Printf("\nDynamic cols\n")
		for _, col := range table.Schema {
			if col.IsStatic() {
				continue
			}
			fmt.Printf("| %s %s Padded %t Identity %t RowGUID %t Computed %t Filestream %t",
				col.Name, col.Type, col.IsAnsiPadded, col.IsIdentity, col.IsRowGUIDCol, col.IsComputed, col.IsFilestream)
		}
		fmt.Printf("\n")
	}

}

func (table Table) printTableInfo() {
	fmt.Printf("table index type %s \n", table.indexType)
	fmt.Printf("objectID %d \n",
		table.ObjectId)
	fmt.Printf("Partition ids:\n")
	for _, partitionId := range table.AllocationUnitIdTopartitionId {
		fmt.Printf("%d \n", partitionId)
	}

	fmt.Print("Allocation unit ids \n")
	for allocationUnitId := range table.AllocationUnitIdTopartitionId {
		fmt.Printf("%d \n", allocationUnitId)
	}

}

func (table Table) Show(showSchema bool, showContent bool,
	showAllocation string, showIndex bool, tabletype string, showtorow int, skiprows int,
	showrows []int, showcarved bool, showtableldf bool, showcolnames []string, showrawdata bool) {

	if showSchema || showContent || showtableldf || showAllocation != "" {
		fmt.Printf("Table %s\n", table.Name)
	}
	if showSchema {
		table.printSchema()
	}
	if showContent {

		fmt.Printf("(l) - logged (c) - carved\n")
		table.printHeader(showcolnames, showtableldf)
		table.printData(showtorow, skiprows, showrows, showcarved, showtableldf, showcolnames, showrawdata)
		table.cleverPrintData()
	}
	if showtableldf {
		table.printLog()
	}

	if showIndex {
		table.printIndex()
	}

	switch showAllocation {
	case "sorted":
		table.printAllocationSorted()
	case "links":
		table.printAllocationWithLinks()
	case "simple":
		table.printAllocation()
	}

}

func (table Table) printLog() {

	for _, record := range table.logRecords {
		record.ShowLOPInfo("any")
	}

}

func (table Table) printAllocationWithLinks() {
	table.printTableInfo()

	fmt.Print("Page Ids\n")

	for pageType, pagesType := range table.PageIDsPerType {
		slices.Sort(pagesType)
		if len(pagesType) == 0 {
			continue
		}
		fmt.Printf("%s", pageType)
		/*for _, pageId := range pagesType {
			fmt.Printf(" %d <- %d -> %d", pages[pageId].GetPrevPage(), pageId, pages[pageId].GetNextPage())
		}*/
		fmt.Print("\n")
	}
	fmt.Print("\n")

}

func (table Table) printAllocation() {
	table.printTableInfo()

	fmt.Print("Page Ids\n")

	for pageType, pagesType := range table.PageIDsPerType {

		if len(pagesType) == 0 {
			continue
		}
		fmt.Printf("%s", pageType)
		for _, pageId := range pagesType {
			fmt.Printf(" %d\n", pageId)
		}
		fmt.Print("\n")
	}

}

func (table Table) printAllocationSorted() {
	table.printTableInfo()

	fmt.Print("Page Ids\n")

	for pageType, pagesType := range table.PageIDsPerType {
		slices.Sort(pagesType)

		if len(pagesType) == 0 {
			continue
		}
		fmt.Printf("%s", pageType)
		for _, pageId := range pagesType {
			fmt.Printf(" %d", pageId)
		}
		fmt.Print("\n")
	}
	fmt.Print("\n")

}

func (table Table) GetHeader(colnames []string) []string {
	var names []string
	for _, c := range table.Schema {

		for _, colname := range colnames {
			if colname != "" && colname != c.Name {
				continue
			}
			names = append(names, c.Name)
		}
		if len(colnames) == 0 {
			names = append(names, c.Name)
		}
	}
	names = append(names, "log information")
	return names

}

func (table Table) GetRecords(wg *sync.WaitGroup, selectedRows []int, colnames []string,
	records chan<- utils.Record) {
	defer wg.Done()

	locatedRow := true

	for rowidx, row := range table.Rows {
		var record utils.Record
		var vals []string

		for _, rownum := range selectedRows {
			if rowidx+1 == rownum {
				locatedRow = true
				break
			} else {
				locatedRow = false
			}
		}

		if len(selectedRows) != 0 && !locatedRow {
			continue
		}

		for _, c := range table.Schema {

			if len(colnames) == 0 {
				colData := row.ColMap[c.Name]

				vals = append(vals, c.ToString(colData.Content))
			}

			for _, colname := range colnames {
				if colname != "" && colname != c.Name {
					continue
				}
				colData := row.ColMap[c.Name]
				if c.IsComputed && !c.IsPersisted {
					vals = append(vals, c.Computed.Definition)
					continue
				}
				vals = append(vals, c.ToString(colData.Content))
			}

		}
		vals = append(vals, row.LoggedOperation)
		record = utils.Record{Vals: vals, Carved: row.Carved, Logged: row.Logged,
			LoggedOperation: row.LoggedOperation}

		records <- record
	}
	close(records)
}

func (table Table) GetBlobs(wg *sync.WaitGroup, blobs chan<- utils.Blob) {
	defer wg.Done()
	for rowid, row := range table.Rows {

		for _, c := range table.Schema {
			if c.Type != "image" && c.Type != "varbinary" {
				continue
			}
			colData := row.ColMap[c.Name]

			blobs <- utils.Blob{Name: c.Name, Content: colData.Content, Id: rowid}
		}
	}
	close(blobs)

}

func (table Table) printHeader(showcolnames []string, showldf bool) {
	for _, c := range table.Schema {
		if len(showcolnames) == 0 {
			fmt.Printf("%s ", c.Name)
			continue
		}
		for _, showcolname := range showcolnames {
			if showcolname != "" && showcolname != c.Name {
				continue
			}
			fmt.Printf("%s ", c.Name)
		}

	}
	if showldf {
		fmt.Printf("log information ")
	}
	fmt.Printf("\n")
}

func (table Table) printIndex() {
	fmt.Printf("Table Index names\n")
	for _, tindex := range table.Indexes {
		if tindex.IsClustered {
			fmt.Printf(" Clustered ")
		} else {
			fmt.Printf(" Statistics ")
		}
		fmt.Printf("%s \n cols:", tindex.Name)
		for _, c := range tindex.Columns {
			fmt.Printf("%s ", c.Name)
		}

		fmt.Printf("\n")

		for idx, row := range tindex.Rows {
			fmt.Printf("%d: ", idx+1)
			for _, c := range tindex.Columns {
				colData := row.ColMap[c.Name]
				c.Print(colData.Content)
			}
			fmt.Printf("\n")
		}
	}

}

func (table Table) cleverPrintData() {
	groupedRowsById := make(map[string]tables.Row)

	for _, row := range table.Rows {
		c := table.Schema[0]
		colData := row.ColMap[c.Name]

		groupedRowsById[c.ToString(colData.Content)] = *row

	}

	for _, row := range table.Rows {
		c := table.Schema[0]
		colData := row.ColMap[c.Name]

		groupedRowsById[c.ToString(colData.Content)] = *row
	}

	/*fmt.Printf("\nGrouped By First col all changes carved and logged oldest first\n")
	sort.Sort(ByActionDate(table.Rows))
	for _, loggedRow := range table.loggedrows {
		for cid, c := range table.Schema {
			loggedCol := loggedrow.ColMap[c.Name]
			if cid == 0 {

				org_row = groupedRowsById[c.toString(loggedCol.Content)] //to check arbitrary
			}
			if org_row.ColMap != nil {
				orgData := c.toString(org_row.ColMap[c.Name].Content)
				loggedData := c.toString(loggedCol.Content)

				if loggedData != orgData {
					fmt.Printf(" ** %s -> %s ", loggedData, orgData)

				} else if loggedCol.LoggedColData != nil {
					fmt.Printf(" **  %s --> %s ",
						c.toString(loggedCol.LoggedColData.Content), orgData)

				} else {
					fmt.Printf(" %s ", orgData)
				}

			}

		}

		fmt.Printf("%s \n", loggedRow.LoggedOperation)

	}*/
}

func (table Table) printData(showtorow int, skiprows int,
	showrows []int, showcarved bool, showldf bool, showcolnames []string, showrawdata bool) {
	for idx, row := range table.Rows { // when no rder check?
		locatedRow := true

		if skiprows != -1 && idx+1 < skiprows {
			continue
		}
		if showtorow != -1 && idx+1 > showtorow {
			break
		}

		for _, rownum := range showrows {

			if idx+1 == rownum {
				locatedRow = true
				break
			} else {
				locatedRow = false
			}

		}

		if len(showrows) != 0 && !locatedRow {
			continue
		}

		if showcarved && row.Carved && !row.Logged {
			fmt.Printf("(c) %d: ", idx+1)
		} else if !showcarved && row.Carved {
			continue
		}

		if showcarved && row.Carved && showldf && row.Logged {
			fmt.Printf("(c)(l) %d: ", idx+1)
		} else if showldf && row.Logged {
			fmt.Printf("(l) %d: ", idx+1)
		} else if !row.Carved {
			fmt.Printf("%d: ", idx+1)
		}

		for _, c := range table.Schema {

			for _, showcolname := range showcolnames {
				if showcolname != "" && showcolname != c.Name {
					continue
				}
				colData := row.ColMap[c.Name]
				if c.IsComputed && !c.IsPersisted {
					c.Computed.Print()
				} else {
					c.Print(colData.Content)
				}

				if showldf && colData.LoggedColData != nil {
					fmt.Printf(" -> ")
					c.Print(colData.LoggedColData.Content)
				}

				if showrawdata {
					fmt.Printf("%x\n", colData.Content)
				}

			}

		}

		if showldf && row.Logged {
			fmt.Printf(" %s ", row.LoggedOperation)
		}

		fmt.Printf("\n")

	}

}

func (table *Table) updateColOffsets(column_id uint32, offset int16, parirtitionId uint64) error {
	if len(table.Schema) < int(column_id) {
		msg := fmt.Sprintf("Partition columnd id %d exceeds nof cols %d of table %s", column_id, len(table.Schema), table.Name)
		mslogger.Mslogger.Warning(msg)
		return errors.New(msg)
	} else if column_id < 1 {
		msg := fmt.Sprintf("Column Id is less than one %d,\n", column_id)
		mslogger.Mslogger.Warning(msg)
		return errors.New(msg)
	} else if offset < 4 {
		msg := fmt.Sprintf("Offset %d of col %s of table %s is less than the minimum allowed offset of 4", offset,
			table.Schema[column_id-1].Name, table.Name)
		mslogger.Mslogger.Warning(msg)
		return errors.New(msg)
	} else {
		msg := fmt.Sprintf("Updated offset %d of col %s of table %s", offset,
			table.Schema[column_id-1].Name, table.Name)
		mslogger.Mslogger.Info(msg)
		table.Schema[column_id-1].OffsetMap[parirtitionId] = offset
		return nil
	}

}

func (table *Table) setContent(dataPages page.PagesPerId[uint32],
	lobPages page.PagesPerId[uint32], textLobPages page.PagesPerId[uint32]) int {
	forwardPages := map[uint32][]uint32{} //list by when seen forward pointer with parent page

	rownum := 0
	if table.PageIDsPerType["IndexedDATA"] != nil {
		for _, pageId := range table.PageIDsPerType["IndexedDATA"] {
			pagesPerIDNode := dataPages.Lookup[pageId]
			if pagesPerIDNode == nil {
				logger.Mslogger.Warning(fmt.Sprintf("page Id not found in data pages %d", pageId))
				continue
			}
			page := pagesPerIDNode.Pages[0]

			rownum += table.setContentFromPage(page, lobPages, textLobPages, forwardPages)

		}
	} else {
		node := dataPages.GetHeadNode()
		for node != nil {
			page := node.Pages[0]
			rownum += table.setContentFromPage(page, lobPages, textLobPages, forwardPages)
			node = node.Next
		}

	}
	return rownum
}

func (table *Table) setContentFromPage(page page.Page,
	lobPages page.PagesPerId[uint32], textLobPages page.PagesPerId[uint32],
	forwardPages map[uint32][]uint32) int {
	pageId := page.Header.PageId
	if page.HasForwardingPointers() {
		forwardPages[page.Header.PageId] = page.FollowForwardingPointers()

	}

	table.indexType = page.GetIndexType()
	pageAllocationUnitId := page.Header.GetMetadataAllocUnitId()
	partitionId := table.AllocationUnitIdTopartitionId[pageAllocationUnitId]

	nofCols := len(table.Schema)
	pageRows := 0
	for _, datarow := range page.DataRows {

		if datarow.Carved && datarow.NullBitmap == nil {
			msg := fmt.Sprintf("Null Bitmap in carved  in row %d,  page %d and schema cols %d table %s",
				pageRows, pageId, nofCols, table.Name)
			mslogger.Mslogger.Warning(msg)
			continue
		}

		if int(datarow.NumberOfCols) != nofCols { // mismatch data page and table schema!
			msg := fmt.Sprintf("Mismatch in number of data cols %d in row %d,  page %d and schema cols %d table %s",
				int(datarow.NumberOfCols), pageRows, pageId, nofCols, table.Name)
			mslogger.Mslogger.Warning(msg)
			//continue
		}
		if datarow.VarLenCols != nil && int(datarow.NumberOfVarLengthCols) != len(*datarow.VarLenCols) {
			msg := fmt.Sprintf("Mismatch in number of declared data var cols %d in row %d,  page %d and with actual cols %d table %s",
				int(datarow.NumberOfVarLengthCols), pageRows, pageId, len(*datarow.VarLenCols), table.Name)
			mslogger.Mslogger.Warning(msg)
			//continue
		}

		if datarow.HasVersionTag() {
			msg := fmt.Sprintf("Datarow %d at pageId %d has versioning enabled. Table %s",
				pageRows, pageId, table.Name)
			mslogger.Mslogger.Warning(msg)

		}

		row := table.ProcessRow(datarow, lobPages, textLobPages, partitionId)
		row.RowId = utils.RowId{PageId: pageId, SlotNumber: uint16(pageRows)}
		table.Rows = append(table.Rows, &row)
		pageRows++
	}
	return pageRows
}

func (table Table) ProcessRow(datarow data.DataRow,
	lobPages page.PagesPerId[uint32], textLobPages page.PagesPerId[uint32], partitionId uint64) tables.Row {

	colMap := make(tables.ColMap)
	nofCols := len(table.Schema)
	bitrepresentation := datarow.PrintNullBitmapToBit(nofCols)

	nofNullCols := 0 // only null var cols
	computedCols := 0

	for colnum, col := range table.Schema {
		//schema is sorted by colorder use colnum instead of col.Order
		if colnum+1 != int(col.Order) {
			mslogger.Mslogger.Warning(fmt.Sprintf("Discrepancy possible column %s deletion %d order %d !", col.Name, colnum+1, col.Order))
		}
		if col.IsComputed {
			computedCols++
			continue //computed cols are not stored
		}
		//check only when number of cols equal to nofCols
		if colnum < int(datarow.NumberOfCols) &&
			utils.HasFlagSet(bitrepresentation, colnum+1-computedCols) { //col is NULL skip when ASCII 49  (1)
			//computed cols are not stored

			//msg := fmt.Sprintf(" %s SKIPPED  %d  type %s ", col.Name, col.Order, col.Type)
			//mslogger.Mslogger.Error(msg)

			nofNullCols++
			continue
		}

		//mslogger.Mslogger.Info(col.Name + " " + fmt.Sprintf("%s %d %s %d", col.IsStatic(), col.Order, col.Type, col.Size))
		colval, e := col.AddContent(datarow, lobPages, textLobPages, partitionId, nofNullCols)
		if e == nil {
			colMap[col.Name] = tables.ColData{Content: colval}
		}
	}
	return tables.Row{ColMap: colMap, Carved: datarow.Carved, Logged: false}
}
