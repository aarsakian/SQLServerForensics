package db

import (
	LDF "MSSQLParser/ldf"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Row struct {
	ColMap          ColMap
	LoggedOperation string
	LogDate         time.Time
}

type TableIndex struct {
	id          uint32
	name        string
	rootPageId  uint32
	firstPageId uint32
	isClustered bool
	columns     []*Column
	rowMap      map[uint32]Row
}

type Table struct {
	Name                          string
	ObjectId                      int32
	Type                          string
	rows                          []Row
	carvedrows                    []Row
	loggedrows                    []Row
	AllocationUnitIdTopartitionId map[uint64]uint64
	Schema                        []Column
	Indexes                       []TableIndex
	VarLenCols                    []int
	PageIDsPerType                map[string][]uint32 //pageType ->pageID
	indexType                     string
	orderedRows                   []*Row
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
		for idx, col := range table.Schema {
			if col.Order == uint16(sysiscol.Intprop) {

				table.Indexes[sysiscol.Idminor-1].columns = append(table.Indexes[sysiscol.Idminor-1].columns,
					&table.Schema[idx])
			}

		}
	}
}

func (table *Table) AddChangesHistory(pagesPerAllocUnitID page.PagesPerId[uint64], carvedLogRecords LDF.Records, activeLogRecords LDF.Records) {
	var allocatedPages page.Pages

	var candidateRecords LDF.Records
	for allocUnitID := range table.AllocationUnitIdTopartitionId {
		allocatedPages = pagesPerAllocUnitID.GetPages(allocUnitID)
	}

	lop_mod_ins_del_records := carvedLogRecords.FilterByOperations(
		[]string{"LOP_INSERT_ROW", "LOP_DELETE_ROW", "LOP_MODIFY_ROW"})

	lop_mod_ins_del_records = append(lop_mod_ins_del_records,
		activeLogRecords.FilterByOperations(
			[]string{"LOP_INSERT_ROW", "LOP_DELETE_ROW", "LOP_MODIFY_ROW"})...)

	for _, page := range allocatedPages {
		if page.GetType() != "DATA" {
			continue
		}
		candidateRecords = append(candidateRecords,
			lop_mod_ins_del_records.FilterByPageID(page.Header.PageId)...)
	}

	sort.Sort(LDF.ByDecreasingLSN(candidateRecords))
	table.addLogChanges(candidateRecords)

}

func (tableIndex *TableIndex) addAllocatedPages(sysallocunit SysAllocUnits) {
	rooPageId := sysallocunit.GetRootPageId()
	if rooPageId == 0 {
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

func (table *Table) setIndexContent(indexPages page.PagesPerId[uint32]) {

	var pagesStack []uint32

	var pages *page.PagesPerIdNode

	for idx, tindex := range table.Indexes {
		table.Indexes[idx].rowMap = make(map[uint32]Row)

		pagesStack = append(pagesStack, tindex.rootPageId)

		for len(pagesStack) != 0 && pagesStack[0] != 0 {

			pageId := pagesStack[0]

			pagesStack = pagesStack[1:] //pop
			pages = indexPages.Lookup[pageId]
			page := pages.Pages[0]
			for _, indexrow := range page.IndexRows {

				cmap := ColMap{}

				startOffset := 0

				for _, c := range tindex.columns {
					cmap[c.Name] = ColData{Content: indexrow.NoNLeaf.KeyValue[startOffset : startOffset+int(c.Size)]}
					startOffset += int(c.Size)
				}

				_, ok := indexPages.Lookup[indexrow.NoNLeaf.ChildPageID]
				if ok {
					pagesStack = append(pagesStack, indexrow.NoNLeaf.ChildPageID)
				}

				table.Indexes[idx].rowMap[indexrow.NoNLeaf.ChildPageID] = Row{ColMap: cmap}
			}

		}

	}

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

		for rowid, row := range table.rows {
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
				table.orderedRows = append(table.orderedRows, &table.rows[rowid])
			}

		}

	}

	*/

}

func (table *Table) AddRow(record LDF.Record) {

	lobPages := page.PagesPerId[uint32]{}
	textLobPages := page.PagesPerId[uint32]{}
	colmap := make(ColMap)

	for _, col := range table.Schema {
		if record.Lop_Insert_Delete.DataRow == nil {
			msg := fmt.Sprintf("Lop Insert Record missing DataRow %s", record.CurrentLSN.ToStr())
			mslogger.Mslogger.Warning(msg)
			continue
		}

		colval, e := col.addContent(*record.Lop_Insert_Delete.DataRow, lobPages, textLobPages, record.Lop_Insert_Delete.PartitionID)
		if e == nil {
			colmap[col.Name] = ColData{Content: colval}
		}

	}

	loggedOperation := "Inserted at "
	loggedOperation += record.GetBeginCommitDate()
	loggedOperation += fmt.Sprintf(" commited at %s", record.GetEndCommitDate())

	table.loggedrows = append(table.loggedrows, Row{ColMap: colmap, LoggedOperation: loggedOperation,
		LogDate: record.GetBeginCommitDateObj()})
}

func (table *Table) MarkRowDeleted(record LDF.Record) {
	rowid := int(record.Lop_Insert_Delete.RowId.SlotNumber)
	loggedOperation := "Deleted at " + record.GetBeginCommitDate() +
		fmt.Sprintf(" commited at %s", record.GetEndCommitDate())

	row := table.rows[rowid]
	row.LoggedOperation = loggedOperation
	row.LogDate = record.GetBeginCommitDateObj()

	table.loggedrows = append(table.loggedrows, row)

}

func (table *Table) MarkRowModified(record LDF.Record) {

	rowid := int(record.Lop_Insert_Delete.RowId.SlotNumber)
	row := table.rows[rowid]
	row.LoggedOperation += "Modified at " + record.GetBeginCommitDate() + fmt.Sprintf(" commited at %s", record.GetEndCommitDate())
	row.LogDate = record.GetBeginCommitDateObj()

	for _, c := range table.Schema {
		if c.OffsetMap[record.Lop_Insert_Delete.PartitionID] >= int16(record.Lop_Insert_Delete.OffsetInRow) {
			var newcontent bytes.Buffer
			newcontent.Grow(int(c.Size))

			colData := row.ColMap[c.Name]
			//new data from startoffset -> startoffset + modifysize
			startOffset := int16(record.Lop_Insert_Delete.OffsetInRow) - c.OffsetMap[record.Lop_Insert_Delete.PartitionID]

			newcontent.Write(colData.Content[:startOffset]) //unchanged content
			newcontent.Write(record.Lop_Insert_Delete.RowLogContents[0])
			newcontent.Write(colData.Content[startOffset+int16(record.Lop_Insert_Delete.ModifySize):])

			colData.LoggedColData = &ColData{Content: newcontent.Bytes()}
			row.ColMap[c.Name] = colData

			break
		}

	}
	table.loggedrows = append(table.loggedrows, row)

}

func (table *Table) addLogChanges(records LDF.Records) {
	for rowid := range table.rows {
		for _, record := range records {
			if uint16(rowid) != record.Lop_Insert_Delete.RowId.SlotNumber {
				continue
			}

			if record.GetOperationType() == "LOP_INSERT_ROW" {
				table.AddRow(record)
			} else if record.GetOperationType() == "LOP_DELETE_ROW" {
				table.MarkRowDeleted(record)
			} else if record.GetOperationType() == "LOP_MODIFY_ROW" {
				table.MarkRowModified(record)
			}

		}
	}

}

func (table Table) getHeader(colnames []string) utils.Record {
	var record utils.Record
	for _, c := range table.Schema {
		for _, colname := range colnames {
			if colname != "" && colname != c.Name {
				continue
			}
			record = append(record, c.Name)
		}

	}
	return record
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
		table.Schema[idx].VarLenOrder = uint16(vid)

		vid++
	}
}

func (table *Table) addColumns(columns []SysColpars) {

	for _, col := range columns {
		table.addColumn(Column{Name: col.GetName(), Type: col.GetType(),
			Size: col.Length, Order: col.Colid, CollationId: col.Collationid,
			Precision: col.Prec, Scale: col.Scale, OffsetMap: map[uint64]int16{}})
	}
	table.setVarLenCols()

}

func (table Table) printSchema() {
	if table.Schema != nil {

		fmt.Printf("Static cols \n")
		for _, col := range table.Schema {
			if !col.isStatic() {
				continue
			}
			fmt.Printf(" | %s %s", col.Name, col.Type)
		}
		fmt.Printf("\nDynamic cols\n")
		for _, col := range table.Schema {
			if col.isStatic() {
				continue
			}
			fmt.Printf("| %s %s", col.Name, col.Type)
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
	showAllocation string, showIndex bool, tabletype string, showrows int, skiprows int,
	showrow int, showcarved bool, showldf bool, showcolnames []string, showrawdata bool) {

	fmt.Printf("\nTable %s \n", table.Name)
	if showSchema {
		table.printSchema()
	}
	if showContent {
		table.printHeader(showcolnames)
		table.printData(showrows, skiprows, showrow, showcarved, showldf, showcolnames, showrawdata)
		table.cleverPrintData()
	}

	if showIndex {
		table.printIndex()
	}

	if showAllocation == "simple" {

		table.printAllocation()
	} else if showAllocation == "links" {
		table.printAllocationWithLinks()
	}

}

func (table Table) printAllocationWithLinks() {
	table.printTableInfo()

	fmt.Print("Page Ids\n")

	for pageType, pagesType := range table.PageIDsPerType {
		sort.Slice(pagesType, func(i, j int) bool {
			return pagesType[i] < pagesType[j]
		})
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
		sort.Slice(pagesType, func(i, j int) bool {
			return pagesType[i] < pagesType[j]
		})
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

func (table Table) GetRecords(wg *sync.WaitGroup, selectedRow int, colnames []string, records chan<- utils.Record) {
	defer wg.Done()

	records <- table.getHeader(colnames)

	for rownum, row := range table.rows {
		var record utils.Record
		if selectedRow != -1 && selectedRow != rownum {
			continue
		}
		for _, c := range table.Schema {
			for _, colname := range colnames {
				if colname != "" && colname != c.Name {
					continue
				}
				colData := row.ColMap[c.Name]
				record = append(record, c.toString(colData.Content))
			}

		}
		records <- record
	}
	close(records)
}

func (table Table) GetImages() utils.Images {
	var images utils.Images

	for _, row := range table.rows {

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
		fmt.Printf("%s Name \n cols:", tindex.name)
		for _, c := range tindex.columns {
			fmt.Printf("%s ", c.Name)
		}

		fmt.Printf("\n")
	}

}

func (table Table) cleverPrintData() {
	groupedRowsById := make(map[string]Row)
	var org_row Row
	for _, row := range table.rows {
		c := table.Schema[0]
		colData := row.ColMap[c.Name]

		groupedRowsById[c.toString(colData.Content)] = row

	}

	for _, row := range table.carvedrows {
		c := table.Schema[0]
		colData := row.ColMap[c.Name]

		groupedRowsById[c.toString(colData.Content)] = row
	}

	fmt.Printf("\nGrouped By First col all changes carved and logged oldest first\n")
	sort.Sort(ByActionDate(table.loggedrows))
	for _, row := range table.loggedrows {
		for cid, c := range table.Schema {
			loggedCol := row.ColMap[c.Name]
			if cid == 0 {

				org_row = groupedRowsById[c.toString(loggedCol.Content)]
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

		fmt.Printf("%s \n", row.LoggedOperation)

	}
}

func (table Table) printData(showtorow int, skiprows int,
	showrow int, showcarved bool, showldf bool, showcolnames []string, showrawdata bool) {

	for idx, row := range table.orderedRows { // when no rder check?
		if skiprows != -1 && idx < skiprows {
			continue
		}
		if showtorow != -1 && idx > showtorow {
			break
		}

		if showrow != -1 && idx != showrow {
			continue
		}

		for _, c := range table.Schema {
			for _, showcolname := range showcolnames {
				if showcolname != "" && showcolname != c.Name {
					continue
				}
				colData := row.ColMap[c.Name]
				c.Print(colData.Content)
				if showrawdata {
					fmt.Printf("%x\n", colData.Content)
				}

			}
		}
		fmt.Printf("\n")
	}

	if showcarved {
		fmt.Printf("* = carved row\n")
		for _, row := range table.carvedrows {
			for cid, c := range table.Schema {
				for _, showcolname := range showcolnames {
					if showcolname != "" && showcolname != c.Name {
						continue
					}
					colData := row.ColMap[c.Name]
					if cid == 0 {
						fmt.Printf("* ")
					}
					c.Print(colData.Content)

				}

			}
			fmt.Printf("\n")
		}
	}

	if showldf {
		fmt.Printf("\n** = logged row\n")

		for _, row := range table.loggedrows {
			for cid, c := range table.Schema {

				colData := row.ColMap[c.Name]
				if cid == 0 {
					fmt.Printf("** ")
				}

				if colData.LoggedColData != nil {
					fmt.Printf(" -> ")
					c.Print(colData.LoggedColData.Content)
				}
				c.Print(colData.Content)

			}

			fmt.Printf("%s \n", row.LoggedOperation)
		}
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

			table.rows = append(table.rows,
				table.ProcessRow(rownum, datarow, lobPages, textLobPages, partitionId))

		}

		for _, datarow := range page.CarvedDataRows {
			if int(datarow.NumberOfCols) != nofCols { // mismatch data page and table schema!
				msg := fmt.Sprintf("Mismatch in number of data cols %d in row %d,  page %d and schema cols %d table %s",
					int(datarow.NumberOfCols), rownum, pageId, nofCols, table.Name)
				mslogger.Mslogger.Warning(msg)
				continue
			}
			if datarow.VarLenCols != nil && int(datarow.NumberOfVarLengthCols) != len(*datarow.VarLenCols) {
				msg := fmt.Sprintf("Mismatch in var cols! Investigate page %d row %d. Declaring %d in reality %d table %s",
					pageId, rownum, int(datarow.NumberOfVarLengthCols), len(*datarow.VarLenCols), table.Name)
				mslogger.Mslogger.Warning(msg)
				continue
			}
			rownum++

			table.carvedrows = append(table.carvedrows,
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

	for colnum, col := range table.Schema {
		//schema is sorted by colorder use colnum instead of col.Order
		if colnum+1 != int(col.Order) {
			mslogger.Mslogger.Warning(fmt.Sprintf("Discrepancy possible column %s deletion %d order %d !", col.Name, colnum+1, col.Order))
		}
		//check only when number of cols equal to nofCols
		if colnum < int(datarow.NumberOfCols) && utils.HasFlagSet(bitrepresentation, colnum+1) { //col is NULL skip when ASCII 49  (1)

			//msg := fmt.Sprintf(" %s SKIPPED  %d  type %s ", col.Name, col.Order, col.Type)
			//mslogger.Mslogger.Error(msg)
			continue
		}

		//mslogger.Mslogger.Info(col.Name + " " + fmt.Sprintf("%s %d %s %d", col.isStatic(), col.Order, col.Type, col.Size))
		colval, e := col.addContent(datarow, lobPages, textLobPages, partitionId)
		if e == nil {
			colmap[col.Name] = ColData{Content: colval}
		}
	}
	return Row{ColMap: colmap}
}
