package db

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"fmt"
	"sort"
)

type Table struct {
	Name              string
	ObjectId          int32
	Type              string
	rows              []ColMap
	PartitionIds      []uint64
	AllocationUnitIds []uint64
	Schema            []Column
	VarLenCols        []int
	PageIds           map[string][]uint32
	indexType         string
}

func (table Table) getHeader() utils.Record {
	var record utils.Record
	for _, c := range table.Schema {
		record = append(record, c.Name)
	}
	return record
}

func (table *Table) addColumn(name string, coltype string, size int16, order uint16, collationId uint32, prec uint8, scale uint8) {
	col := Column{Name: name, Type: coltype, Size: size, Order: order, CollationId: collationId, Precision: prec, Scale: scale}
	table.Schema = append(table.Schema, col)

}

func (table *Table) addColumns(results []page.Result[string, string, int16, uint16, uint32, uint8, uint8]) {

	for _, res := range results {
		table.addColumn(res.First, res.Second, res.Third, res.Fourth, res.Fifth, res.Sixth, res.Seventh)
	}

}

func (table *Table) updateVarLenCols() {

	vid := 0
	//colorder := uint16(1)
	// first arrange static

	/*	for idx := range table.Schema {
		if table.Schema[idx].isStatic() {
			table.Schema[idx].Order = colorder
			table.Schema[idx].VarLenOrder = 0
			colorder++
		}
	}*/

	//2nd pass for var len cols
	for idx := range table.Schema {
		if table.Schema[idx].isStatic() {
			continue
		}
		table.Schema[idx].VarLenOrder = uint16(vid)

		vid++

	}
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

	for pageType, pagesType := range table.PageIds {
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

	for pageType, pagesType := range table.PageIds {
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

func (table Table) GetRecords() utils.Records {
	var records utils.Records

	records = append(records, table.getHeader())

	for _, row := range table.rows {
		var record utils.Record
		for _, c := range table.Schema {
			colData := row[c.Name]
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
			colData := row[c.Name]

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

func (table Table) printData(showtorow int, showrow int) {
	for idx, row := range table.rows {
		if showtorow != -1 && idx > showtorow {
			break
		}

		if showrow != -1 && idx != showrow {
			continue
		}
		for _, c := range table.Schema {
			colData := row[c.Name]
			if colData.Carved {
				fmt.Printf("* ")
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
	rowid := 0
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
			table.ProcessRow(rowid, datarow, pageId, lobPages, textLobPages, carved)
			rowid++
		}

		for _, datarow := range page.CarvedDataRows {
			rowid++
			carved = true
			table.ProcessRow(rowid, datarow, pageId, lobPages, textLobPages, carved)

		}
		node = node.Next
	}

}

func (table *Table) ProcessRow(rowid int, datarow page.DataRow, pageId uint32,
	lobPages page.PagesPerId[uint32], textLobPages page.PagesPerId[uint32], carved bool) {
	m := make(ColMap)

	nofCols := len(table.Schema)

	if int(datarow.NumberOfCols) != nofCols { // mismatch data page and table schema!
		msg := fmt.Sprintf("Mismatch in number of data cols %d in row %d,  page %d and schema cols %d table %s",
			int(datarow.NumberOfCols), rowid, pageId, nofCols, table.Name)
		mslogger.Mslogger.Warning(msg)
		return
	}
	if datarow.VarLenCols != nil && int(datarow.NumberOfVarLengthCols) != len(*datarow.VarLenCols) {
		msg := fmt.Sprintf("Mismatch in var cols! Investigate page %d row %d. Declaring %d in reality %d table %s",
			pageId, rowid, int(datarow.NumberOfVarLengthCols), len(*datarow.VarLenCols), table.Name)
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
			m[col.Name] = ColData{Content: colval, Carved: carved}
		}
	}
	table.rows = append(table.rows, m)
}

func (t Table) Less(i, j int) bool {
	c := t.Schema
	return c[i].Order < c[j].Order
}

func (t Table) Swap(i, j int) {
	c := t.Schema
	c[i], c[j] = c[j], c[i]
}

func (t Table) Len() int {
	return len(t.Schema)

}
