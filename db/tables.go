package db

import (
	LDF "MSSQLParser/ldf"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"sync"
	"time"
)

type Row struct {
	ColMap          ColMap
	LoggedOperation string
	Carved          bool
	Logged          bool
	LogDate         time.Time
}

type TableIndex struct {
	id          uint32
	name        string
	rootPageId  uint32
	firstPageId uint32
	isClustered bool
	columns     []*Column
	rows        []Row
}

type Table struct {
	Name                          string
	ObjectId                      int32
	Type                          string
	Rows                          []Row
	AllocationUnitIdTopartitionId map[uint64]uint64
	Schema                        []Column
	Indexes                       []TableIndex
	VarLenCols                    []int
	PageIDsPerType                map[string][]uint32 //pageType ->pageID
	indexType                     string
	logRecords                    LDF.Records
}

type ByRowId []ColMap

type ByColOrder []Column

func (b ByColOrder) Less(i, j int) bool {

	return b[i].Order < b[j].Order
}

func (b ByColOrder) Swap(i, j int) {

	b[i], b[j] = b[j], b[i]
}

func (b ByColOrder) Len() int {
	return len(b)

}

type ByActionDate []Row

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

type OrderedRows []Row

/*func (byrowid ByRowId) Len() int {
	return len(byrowid)

}

func (byrowid ByRowId) Less(i, j int) bool {
	return byrowid[i] < byrowid[j]
}

func (byrowid ByRowId) Swap(i, j int) {

	byrowid[i], byrowid[j] = byrowid[j], byrowid[i]
}*/

func (table *Table) udateColIndex(sysiscols SysIsCols) {
	for _, sysiscol := range sysiscols {
		for indexIdx, indexInfo := range table.Indexes {
			if indexInfo.id != sysiscol.Idminor {
				continue
			}
			for idx, col := range table.Schema {
				if col.Order != uint16(sysiscol.Intprop) {
					continue

				}
				table.Indexes[indexIdx].columns = append(table.Indexes[indexIdx].columns,
					&table.Schema[idx])

			}

		}

	}
}

func (table *Table) AddChangesHistory(pagesPerAllocUnitID page.PagesPerId[uint64],
	logRecords LDF.Records) {
	var allocatedPages page.Pages

	var candidateRecords LDF.Records

	// only data changes

	logRecords = logRecords.FilterOutNullOperations()

	for allocUnitID := range table.AllocationUnitIdTopartitionId {
		allocatedPages = append(allocatedPages, pagesPerAllocUnitID.GetPages(allocUnitID)...)
	}

	for _, page := range allocatedPages {
		if page.GetType() != "DATA" {
			continue
		}

		candidateRecords = append(candidateRecords,
			logRecords.FilterByPageID(page.Header.PageId)...)

	}

	sort.Sort(LDF.ByIncreasingLSN(candidateRecords))

	//flag denotes carved
	table.addLogChanges(candidateRecords)

	table.logRecords = candidateRecords

}

func (tableIndex *TableIndex) addAllocatedPages(sysallocunit SysAllocUnits) {
	rooPageId := sysallocunit.GetRootPageId()
	if rooPageId == 0 || sysallocunit.GetDescription() != "IN_ROW_DATA" {
		return
	}

	tableIndex.firstPageId = sysallocunit.GetFirstPageId()
	tableIndex.rootPageId = rooPageId

}

func (table *Table) addIndex(indexInfo SysIdxStats, hasallocunits bool, sysallocunits []SysAllocUnits) {

	tableIndex := TableIndex{id: indexInfo.Indid, name: indexInfo.GetName(), isClustered: indexInfo.Type == 1}

	if hasallocunits {
		for _, sysallocunit := range sysallocunits {
			tableIndex.addAllocatedPages(sysallocunit)

		}
	}

	table.Indexes = append(table.Indexes, tableIndex)

}

func (table *Table) setIndexContent(indexPages page.PagesPerId[uint32]) []uint32 {
	var indexedDataPages []uint32
	for idx := range table.Indexes {
		if !table.Indexes[idx].isClustered {
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

			if tindex.isClustered && int(c.Size) != len(indexrow.NoNLeaf.KeyValue)-4 && //4 bytes to ensure uniqueness of cluster key
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

func (tindex *TableIndex) Populate(indexPages page.PagesPerId[uint32]) []uint32 {
	var rows []Row
	var pagesQueue []uint32

	var pages *page.PagesPerIdNode
	pagesQueue = append(pagesQueue, tindex.rootPageId)

	var keyValue []byte

	var indexedDataPages []uint32
	for len(pagesQueue) != 0 && pagesQueue[0] != 0 {

		pageId := pagesQueue[0]

		pagesQueue = pagesQueue[1:] //pop
		pages = indexPages.Lookup[pageId]
		if pages == nil {
			break
		}
		page := pages.Pages[0]
		for _, indexrow := range page.IndexRows {
			if indexrow.NoNLeaf != nil {
				indexedDataPages = append(indexedDataPages, indexrow.NoNLeaf.ChildPageID)
				pagesQueue = append(pagesQueue, indexrow.NoNLeaf.ChildPageID)

				keyValue = indexrow.NoNLeaf.KeyValue
			} else if indexrow.NoNLeafClustered != nil {
				keyValue = indexrow.NoNLeafClustered.KeyValue

				indexedDataPages = append(indexedDataPages, indexrow.NoNLeafClustered.ChildPageID)
				pagesQueue = append(pagesQueue, indexrow.NoNLeafClustered.ChildPageID)
			} else if indexrow.LeafClustered != nil {
				keyValue = indexrow.LeafClustered.KeyValue
			} else if indexrow.LeafNoNClustered != nil {
				keyValue = indexrow.LeafNoNClustered.KeyValue
			} else {
				continue
			}
			cmap := ColMap{}

			startOffset := 0

			for _, c := range tindex.columns {

				if startOffset > len(keyValue) || startOffset+int(c.Size) > len(keyValue) {
					msg := fmt.Sprintf("data length of non-leaf index is exhausted by %d at page Id %d",
						startOffset+int(c.Size)-len(keyValue), page.Header.PageId)
					mslogger.Mslogger.Warning(msg)
					break
				}

				cmap[c.Name] = ColData{Content: keyValue[startOffset : startOffset+int(c.Size)]}
				startOffset += int(c.Size)
			}

			rows = append(rows, Row{ColMap: cmap})
		}

	}
	tindex.rows = rows

	return indexedDataPages
	/*sort indexes
	slices.SortFunc(rows, func(first, second Row) int {
		var res int
		for cname, fcol := range first.ColMap {
			res = slices.CompareFunc(fcol.Content, second.ColMap[cname].Content,
				func(fbyte byte, sbyte byte) int {
					return cmp.Compare(fbyte, sbyte)
				})
			if res == 0 {
				continue
			}
			return res
		}
		return res

	})
	*/

}

func (table *Table) AddRow(record LDF.Record, carved bool) {

	lobPages := page.PagesPerId[uint32]{}
	textLobPages := page.PagesPerId[uint32]{}
	colmap := make(ColMap)
	nofNullCols := 0
	for _, col := range table.Schema {
		if record.Lop_Insert_Delete.DataRow == nil {
			lsn := record.CurrentLSN.ToStr()
			msg := fmt.Sprintf("Lop Insert Record missing DataRow %s", lsn)
			mslogger.Mslogger.Warning(msg)
			continue
		}

		colval, e := col.addContent(*record.Lop_Insert_Delete.DataRow, lobPages, textLobPages, record.Lop_Insert_Delete.PartitionID, nofNullCols)
		if e == nil {
			colmap[col.Name] = ColData{Content: colval}
		}

	}

	loggedOperation := "Inserted at "
	loggedOperation += record.GetBeginCommitDate()
	loggedOperation += fmt.Sprintf(" commited at %s", record.GetEndCommitDate())

	table.Rows = append(table.Rows, Row{ColMap: colmap, LoggedOperation: loggedOperation,
		LogDate: record.GetBeginCommitDateObj(), Carved: carved, Logged: true})
}

func (table *Table) MarkRowDeleted(record LDF.Record, carved bool) {

	rowid := int(record.Lop_Insert_Delete.RowId.SlotNumber)

	if len(table.Rows) > rowid {

		loggedOperation := "Deleted at " + record.GetBeginCommitDate() +
			fmt.Sprintf(" commited at %s", record.GetEndCommitDate())

		row := table.Rows[rowid]
		row.Carved = carved
		row.Logged = true
		row.LoggedOperation = loggedOperation
		row.LogDate = record.GetBeginCommitDateObj()

		table.Rows[rowid] = row

	}

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
	row := table.ProcessRow(len(table.Rows), *record.Lop_Insert_Delete.DataRow,
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
		table.Rows = append(table.Rows, row)
	}

	return nil
}

func (table *Table) MarkRowModified(record LDF.Record, carved bool) {

	rowid := int(record.Lop_Insert_Delete.RowId.SlotNumber)
	if len(table.Rows) > rowid {
		row := table.Rows[rowid]
		row.LoggedOperation += "Modified at " + record.GetBeginCommitDate() + fmt.Sprintf(" commited at %s", record.GetEndCommitDate())
		row.LogDate = record.GetBeginCommitDateObj()
		row.Carved = carved
		row.Logged = true

		for _, c := range table.Schema {
			if c.OffsetMap[record.Lop_Insert_Delete.PartitionID] >= int16(record.Lop_Insert_Delete.OffsetInRow) {
				var newcontent bytes.Buffer
				newcontent.Grow(int(c.Size))

				colData := row.ColMap[c.Name]
				//new data from startoffset -> startoffset + modifysize
				startOffset := int16(record.Lop_Insert_Delete.OffsetInRow) - c.OffsetMap[record.Lop_Insert_Delete.PartitionID]
				if startOffset > 0 {
					newcontent.Write(colData.Content[:startOffset]) //unchanged content
					newcontent.Write(record.Lop_Insert_Delete.RowLogContents[0])
					newcontent.Write(colData.Content[startOffset+int16(record.Lop_Insert_Delete.ModifySize):])

					colData.LoggedColData = &ColData{Content: newcontent.Bytes()}
					row.ColMap[c.Name] = colData

				}

				break
			}

		}
		table.Rows = append(table.Rows, row)
	}

}

func (table *Table) addLogChanges(records LDF.Records) {
	groupedPerSlotID := make(map[int]LDF.Records)
	for _, record := range records {
		if record.Lop_Insert_Delete != nil {
			groupedPerSlotID[int(record.Lop_Insert_Delete.RowId.SlotNumber)] =
				append(groupedPerSlotID[int(record.Lop_Insert_Delete.RowId.SlotNumber)], record)
		} else {
			groupedPerSlotID[int(record.Generic_LOP.RowId.SlotNumber)] =
				append(groupedPerSlotID[int(record.Generic_LOP.RowId.SlotNumber)], record)
		}

	}
	for _, slotRecordsPerGroup := range groupedPerSlotID {
		for idx, record := range slotRecordsPerGroup {

			if record.GetOperationType() == "LOP_DELETE_ROW" && !slotRecordsPerGroup.HasExpungeOperation(idx) {
				table.MarkRowDeleted(record, record.Carved)
			} else if record.GetOperationType() == "LOP_DELETE_ROW" && slotRecordsPerGroup.HasExpungeOperation(idx) {
				table.AddPurgedRow(record, record.Carved)
			} else if record.GetOperationType() == "LOP_MODIFY_ROW" {
				table.MarkRowModified(record, record.Carved)
			} else if record.GetOperationType() == "LOP_INSERT_ROW" {
				table.AddRow(record, record.Carved)
			}

		}
	}

}

func (table Table) getHeader() utils.Record {
	var names []string
	for _, c := range table.Schema {
		names = append(names, c.Name)

	}
	return utils.Record{Vals: names}
}

func (table *Table) addColumn(column Column) {

	table.Schema = append(table.Schema, column)

}

func (table *Table) setVarLenCols() {

	vid := 0
	for idx := range table.Schema {
		if table.Schema[idx].isStatic() {
			continue
		}
		if table.Schema[idx].IsComputed {
			continue
		}
		table.Schema[idx].VarLenOrder = uint16(vid)

		vid++
	}
}

func (table *Table) addColumns(columns []SysColpars) {

	for _, col := range columns {

		codepage, _, err := utils.LocateEncoding(fmt.Sprintf("%d", col.Collationid))

		if err == nil {

			table.addColumn(Column{Name: col.GetName(), Type: col.GetType(),
				Size: col.Length, Order: col.Colid, CollationId: col.Collationid,
				Charmap:   utils.LocateWindowsCharmap(codepage),
				CodePage:  codepage,
				Precision: col.Prec, Scale: col.Scale,
				OffsetMap:    map[uint64]int16{},
				IsAnsiPadded: col.isAnsiPadded(),
				IsIdentity:   col.isIdentity(),
				IsRowGUIDCol: col.isRowGUIDCol(),
				IsComputed:   col.isComputed(),
				IsFilestream: col.isFilestream(),
			})

		} else {
			table.addColumn(Column{Name: col.GetName(), Type: col.GetType(),
				Size: col.Length, Order: col.Colid, CollationId: col.Collationid,
				Precision: col.Prec, Scale: col.Scale,
				OffsetMap: map[uint64]int16{}, IsAnsiPadded: col.isAnsiPadded(),
				IsIdentity: col.isIdentity(), IsRowGUIDCol: col.isRowGUIDCol(),
				IsComputed: col.isComputed(), IsFilestream: col.isFilestream()})
		}

	}

}

func (table Table) printSchema() {
	if table.Schema != nil {

		fmt.Printf("Static cols \n")
		for _, col := range table.Schema {
			if !col.isStatic() {
				continue
			}
			fmt.Printf(" | %s %s Padded %t Identity %t RowGUID %t Computed %t Filestream %t",
				col.Name, col.Type, col.IsAnsiPadded, col.IsIdentity, col.IsRowGUIDCol, col.IsComputed, col.IsFilestream)
		}
		fmt.Printf("\nDynamic cols\n")
		for _, col := range table.Schema {
			if col.isStatic() {
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

	if showSchema {
		table.printSchema()
	}
	if showContent {

		fmt.Printf("(l) - logged (c) - carved\n")
		table.printHeader(showcolnames)
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

func (table Table) GetRecords(wg *sync.WaitGroup, selectedRows []int, colnames []string, records chan<- utils.Record) {
	defer wg.Done()

	headers := table.getHeader()
	if len(colnames) == 0 {
		records <- headers
	} else {
		var filteredHeaders []string
		for _, headername := range headers.Vals {
			for _, colname := range colnames {
				if colname != "" && colname != headername {
					continue
				}
				filteredHeaders = append(filteredHeaders, headername)
			}
		}
		records <- utils.Record{Vals: filteredHeaders}
	}

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

				vals = append(vals, c.toString(colData.Content))
			}

			for _, colname := range colnames {
				if colname != "" && colname != c.Name {
					continue
				}
				colData := row.ColMap[c.Name]

				vals = append(vals, c.toString(colData.Content))
			}

		}
		record = utils.Record{Vals: vals, Carved: row.Carved, Logged: row.Logged}

		records <- record
	}
	close(records)
}

func (table Table) GetImages() utils.Images {
	var images utils.Images

	for _, row := range table.Rows {

		for _, c := range table.Schema {
			if c.Type != "image" {
				continue
			}
			colData := row.ColMap[c.Name]

			images = append(images, colData.Content)
		}
	}
	return images
}

func (table Table) printHeader(showcolnames []string) {
	for _, c := range table.Schema {
		for _, showcolname := range showcolnames {
			if showcolname != "" && showcolname != c.Name {
				continue
			}
			fmt.Printf("%s ", c.Name)
		}

	}
	fmt.Printf("\n")
}

func (table Table) printIndex() {
	fmt.Printf("Table Index names\n")
	for _, tindex := range table.Indexes {
		if tindex.isClustered {
			fmt.Printf(" Clustered ")
		} else {
			fmt.Printf(" Statistics ")
		}
		fmt.Printf("%s \n cols:", tindex.name)
		for _, c := range tindex.columns {
			fmt.Printf("%s ", c.Name)
		}

		fmt.Printf("\n")

		for idx, row := range tindex.rows {
			fmt.Printf("%d: ", idx+1)
			for _, c := range tindex.columns {
				colData := row.ColMap[c.Name]
				c.Print(colData.Content)
			}
			fmt.Printf("\n")
		}
	}

}

func (table Table) cleverPrintData() {
	groupedRowsById := make(map[string]Row)

	for _, row := range table.Rows {
		c := table.Schema[0]
		colData := row.ColMap[c.Name]

		groupedRowsById[c.toString(colData.Content)] = row

	}

	for _, row := range table.Rows {
		c := table.Schema[0]
		colData := row.ColMap[c.Name]

		groupedRowsById[c.toString(colData.Content)] = row
	}

	/*fmt.Printf("\nGrouped By First col all changes carved and logged oldest first\n")
	sort.Sort(ByActionDate(table.Rows))
	for _, loggedRow := range table.loggedrows {
		for cid, c := range table.Schema {
			loggedCol := loggedRow.ColMap[c.Name]
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
				c.Print(colData.Content)

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
	lobPages page.PagesPerId[uint32], textLobPages page.PagesPerId[uint32]) {
	forwardPages := map[uint32][]uint32{} //list by when seen forward pointer with parent page

	rownum := 0
	node := dataPages.GetHeadNode()
	for node != nil {
		page := node.Pages[0]
		pageId := page.Header.PageId
		if page.HasForwardingPointers() {
			forwardPages[page.Header.PageId] = page.FollowForwardingPointers()

		}

		table.indexType = page.GetIndexType()
		pageAllocationUnitId := page.Header.GetMetadataAllocUnitId()
		partitionId := table.AllocationUnitIdTopartitionId[pageAllocationUnitId]

		nofCols := len(table.Schema)

		for _, datarow := range page.DataRows {

			if datarow.Carved && datarow.NullBitmap == nil {
				msg := fmt.Sprintf("Null Bitmap in carved  in row %d,  page %d and schema cols %d table %s",
					rownum, pageId, nofCols, table.Name)
				mslogger.Mslogger.Warning(msg)
				continue
			}
			rownum++
			if int(datarow.NumberOfCols) != nofCols { // mismatch data page and table schema!
				msg := fmt.Sprintf("Mismatch in number of data cols %d in row %d,  page %d and schema cols %d table %s",
					int(datarow.NumberOfCols), rownum, pageId, nofCols, table.Name)
				mslogger.Mslogger.Warning(msg)
				//continue
			}
			if datarow.VarLenCols != nil && int(datarow.NumberOfVarLengthCols) != len(*datarow.VarLenCols) {
				msg := fmt.Sprintf("Mismatch in number of declared data var cols %d in row %d,  page %d and with actual cols %d table %s",
					int(datarow.NumberOfVarLengthCols), rownum, pageId, len(*datarow.VarLenCols), table.Name)
				mslogger.Mslogger.Warning(msg)
				//continue
			}

			if datarow.HasVersionTag() {
				msg := fmt.Sprintf("Datarow %d at pageId %d has versioning enabled. Table %s",
					rownum, pageId, table.Name)
				mslogger.Mslogger.Warning(msg)

			}

			table.Rows = append(table.Rows,
				table.ProcessRow(rownum, datarow, lobPages, textLobPages, partitionId))

		}

		node = node.Next
	}

}

func (table Table) ProcessRow(rownum int, datarow page.DataRow,
	lobPages page.PagesPerId[uint32], textLobPages page.PagesPerId[uint32], partitionId uint64) Row {

	colmap := make(ColMap)
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

		//mslogger.Mslogger.Info(col.Name + " " + fmt.Sprintf("%s %d %s %d", col.isStatic(), col.Order, col.Type, col.Size))
		colval, e := col.addContent(datarow, lobPages, textLobPages, partitionId, nofNullCols)
		if e == nil {
			colmap[col.Name] = ColData{Content: colval}
		}
	}
	return Row{ColMap: colmap, Carved: datarow.Carved, Logged: false}
}
