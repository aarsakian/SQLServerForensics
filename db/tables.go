package db

import (
	LDF "MSSQLParser/ldf"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"bytes"
	"fmt"
	"sort"
)

type Row struct {
	ColMap          ColMap
	Carved          bool
	LoggedOperation string
}

type Table struct {
	Name              string
	ObjectId          int32
	Type              string
	rows              []Row
	PartitionIds      []uint64
	AllocationUnitIds []uint64
	Schema            []Column
	VarLenCols        []int
	PageIDsPerType    map[string][]uint32 //pageType ->pageID
	indexType         string
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

func (table Table) sortByColOrder() {
	// sort by col order
	sort.Sort(ByColOrder(table.Schema))
}

/*func (byrowid ByRowId) Len() int {
	return len(byrowid)

}

func (byrowid ByRowId) Less(i, j int) bool {
	return byrowid[i] < byrowid[j]
}

func (byrowid ByRowId) Swap(i, j int) {

	byrowid[i], byrowid[j] = byrowid[j], byrowid[i]
}*/

func (table *Table) AddHistoryChanges(record LDF.Record) {

	if record.GetOperationType() == "LOP_DELETE_ROW" {
		affectedRow := table.rows[record.Lop_Insert_Delete.RowId.SlotNumber]
		affectedRow.LoggedOperation = "Deleted at "

		affectedRow.LoggedOperation += record.GetBeginCommitDate()
		affectedRow.LoggedOperation += fmt.Sprintf(" ended at %s", record.GetEndCommitDate())

		table.rows[record.Lop_Insert_Delete.RowId.SlotNumber] = affectedRow

	} else if record.GetOperationType() == "LOP_INSERT_ROW" {

		lobPages := page.PagesPerId[uint32]{}
		textLobPages := page.PagesPerId[uint32]{}
		colmap := make(ColMap)

		for _, col := range table.Schema {

			colval, e := col.addContent(*record.Lop_Insert_Delete.DataRow, lobPages, textLobPages)
			if e == nil {
				colmap[col.Name] = ColData{Content: colval}
			}

		}

		loggedOperation := "Inserted at "
		loggedOperation += record.GetBeginCommitDate()
		loggedOperation += fmt.Sprintf(" ended at %s", record.GetEndCommitDate())

		table.rows = append(table.rows, Row{ColMap: colmap, Carved: false,
			LoggedOperation: loggedOperation})

	} else if record.GetOperationType() == "LOP_MODIFY_ROW" {
		affectedRow := table.rows[record.Lop_Insert_Delete.RowId.SlotNumber]
		affectedRow.LoggedOperation = "Modified at "

		affectedRow.LoggedOperation += record.GetBeginCommitDate()
		affectedRow.LoggedOperation += fmt.Sprintf(" ended at %s", record.GetEndCommitDate())

		for _, c := range table.Schema {
			if c.Offset >= int16(record.Lop_Insert_Delete.OffsetInRow) {
				var newcontent bytes.Buffer
				newcontent.Grow(int(c.Size))

				colData := affectedRow.ColMap[c.Name]
				//new data from startoffset -> startoffset + modifysize
				startOffset := int16(record.Lop_Insert_Delete.OffsetInRow) - c.Offset

				newcontent.Write(colData.Content[:startOffset]) //unchanged content
				newcontent.Write(record.Lop_Insert_Delete.RowLogContents[0])
				newcontent.Write(colData.Content[startOffset+int16(record.Lop_Insert_Delete.ModifySize):])

				colData.LoggedColData = &ColData{Content: newcontent.Bytes()}
				affectedRow.ColMap[c.Name] = colData
				table.rows[record.Lop_Insert_Delete.RowId.SlotNumber] = affectedRow
				break
			}

		}

	}
}

func (table Table) getHeader() utils.Record {
	var record utils.Record
	for _, c := range table.Schema {
		record = append(record, c.Name)
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

func (table *Table) addColumns(columns []page.Result[string, string, int16, uint16, uint32, uint8, uint8]) {

	for _, col := range columns {
		table.addColumn(Column{Name: col.First, Type: col.Second,
			Size: col.Third, Order: col.Fourth, CollationId: col.Fifth,
			Precision: col.Sixth, Scale: col.Seventh})
	}
	table.setVarLenCols()

}

func (table Table) printSchema() {
	if table.Schema != nil {

		fmt.Printf("Table Name:  %s \n", table.Name)
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
	for _, partitionId := range table.PartitionIds {
		fmt.Printf("%d \n", partitionId)
	}

	fmt.Print("Allocation unit ids \n")
	for _, allocationUnitId := range table.AllocationUnitIds {
		fmt.Printf("%d \n", allocationUnitId)
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

func (table Table) GetRecords(selectedRow int) utils.Records {
	var records utils.Records

	records = append(records, table.getHeader())

	for rownum, row := range table.rows {
		var record utils.Record
		if selectedRow != -1 && selectedRow != rownum {
			continue
		}
		for _, c := range table.Schema {
			colData := row.ColMap[c.Name]
			record = append(record, c.toString(colData.Content))

		}
		records = append(records, record)
	}
	return records

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

func (table Table) printHeader() {
	fmt.Printf("\n---------------------------\n")
	for _, c := range table.Schema {
		fmt.Printf("%s ", c.Name)
	}
	fmt.Printf("\n")
}

func (table Table) printData(showtorow int, showrow int, showcarved bool, showldf bool) {

	for idx, row := range table.rows {
		if showtorow != -1 && idx > showtorow {
			break
		}

		if showrow != -1 && idx != showrow {
			continue
		}
		if showldf && row.LoggedOperation != "" {
			fmt.Printf("%s ", row.LoggedOperation)
		}
		for _, c := range table.Schema {

			colData := row.ColMap[c.Name]
			if showcarved && row.Carved {
				fmt.Printf("* ")
			} else if showldf && row.LoggedOperation != "" {
				fmt.Printf("** ")

				if colData.LoggedColData != nil {
					fmt.Printf(" -> ")
					c.Print(colData.LoggedColData.Content)
				}
			}
			c.Print(colData.Content)

		}
		fmt.Printf("\n")
	}

}

func (table *Table) updateColOffsets(column_id int32, offset int16, ordkey int16) {
	if len(table.Schema) < int(column_id) {
		msg := fmt.Sprintf("Partition columnd id %d exceeds nof cols %d of table %s", column_id, len(table.Schema), table.Name)
		mslogger.Mslogger.Warning(msg)
	} else if column_id < 1 {
		msg := fmt.Sprintf("Column Id is less than one %d,\n", column_id)
		mslogger.Mslogger.Warning(msg)
	} else if offset < 4 {
		msg := fmt.Sprintf("Offset %d of col %s of table %s is less than the minimum allowed offset of 4", offset,
			table.Schema[column_id-1].Name, table.Name)
		mslogger.Mslogger.Warning(msg)
	} else {
		table.Schema[column_id-1].Offset = offset
	}

}

func (table *Table) setContent(dataPages page.PagesPerId[uint32],
	lobPages page.PagesPerId[uint32], textLobPages page.PagesPerId[uint32]) {
	forwardPages := map[uint32][]uint32{} //list by when seen forward pointer with parent page
	var carved bool
	rownum := 0
	node := dataPages.GetHeadNode()
	for node != nil {
		page := node.Pages[0]
		pageId := page.Header.PageId
		if page.HasForwardingPointers() {
			forwardPages[page.Header.PageId] = page.FollowForwardingPointers()

		}

		table.indexType = page.GetIndexType()
		for _, datarow := range page.DataRows {
			carved = false
			table.ProcessRow(rownum, datarow, pageId, lobPages, textLobPages, carved)
			rownum++
		}

		for _, datarow := range page.CarvedDataRows {
			rownum++
			carved = true
			table.ProcessRow(rownum, datarow, pageId, lobPages, textLobPages, carved)

		}
		node = node.Next
	}

}

func (table *Table) ProcessRow(rownum int, datarow page.DataRow, pageId uint32,
	lobPages page.PagesPerId[uint32], textLobPages page.PagesPerId[uint32], carved bool) {

	colmap := make(ColMap)
	nofCols := len(table.Schema)

	if int(datarow.NumberOfCols) != nofCols { // mismatch data page and table schema!
		msg := fmt.Sprintf("Mismatch in number of data cols %d in row %d,  page %d and schema cols %d table %s",
			int(datarow.NumberOfCols), rownum, pageId, nofCols, table.Name)
		mslogger.Mslogger.Warning(msg)
		return
	}
	if datarow.VarLenCols != nil && int(datarow.NumberOfVarLengthCols) != len(*datarow.VarLenCols) {
		msg := fmt.Sprintf("Mismatch in var cols! Investigate page %d row %d. Declaring %d in reality %d table %s",
			pageId, rownum, int(datarow.NumberOfVarLengthCols), len(*datarow.VarLenCols), table.Name)
		mslogger.Mslogger.Warning(msg)
		return
	}

	for colnum, col := range table.Schema {
		//schema is sorted by colorder use colnum instead of col.Order
		if colnum+1 != int(col.Order) {
			mslogger.Mslogger.Warning(fmt.Sprintf("Discrepancy possible column %s deletion %d order %d !", col.Name, colnum+1, col.Order))
		}
		if utils.HasFlagSet(datarow.NullBitmap, colnum+1, nofCols) { //col is NULL skip when ASCII 49  (1)

			//msg := fmt.Sprintf(" %s SKIPPED  %d  type %s ", col.Name, col.Order, col.Type)
			//mslogger.Mslogger.Error(msg)
			continue
		}

		//mslogger.Mslogger.Info(col.Name + " " + fmt.Sprintf("%s %d %s %d", col.isStatic(), col.Order, col.Type, col.Size))
		colval, e := col.addContent(datarow, lobPages, textLobPages)
		if e == nil {
			colmap[col.Name] = ColData{Content: colval}
		}
	}
	table.rows = append(table.rows, Row{ColMap: colmap, Carved: carved})
}
