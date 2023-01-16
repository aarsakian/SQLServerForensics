package db

import (
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"fmt"
)

type ColMap map[string][]byte

type Table struct {
	Name          string
	ObjectId      int32
	rows          []ColMap
	Rowsetid      uint64
	PageObjectIds []uint32
	Schema        []Column
	VarLenCols    []int
}

type Column struct {
	Name        string
	Type        string
	Size        uint16
	Order       uint16
	VarLenOrder uint16
}

func (c Column) isStatic() bool {

	if c.Type == "varchar" || c.Type == "nvarchar" || c.Type == "bit" ||
		c.Type == "varbinary" || c.Type == "xml" || c.Type == "text" ||
		c.Type == "ntext" || c.Type == "image" || c.Type == "nchar" ||
		c.Type == "float" || c.Type == "uniqueidentifier" || c.Type == "smallint" ||
		c.Type == "tinyint" {
		return false
	} else {
		return true
	}

}

func (c Column) toString(data []byte) string {
	if c.Type == "varchar" || c.Type == "text" || c.Type == "ntext" {
		return fmt.Sprintf("%s", data)
	} else if c.Type == "int" || c.Type == "tinyint" || c.Type == "bigint" {
		return fmt.Sprintf("%d", utils.ToInt32(data))
	} else {
		return ""
	}
}

func (c *Column) addContent(datarow page.DataRow, skippedVarCols int,
	lobPages page.PageMap, textLOBPages page.PageMap, fixColsOffset int) []byte {
	if datarow.SystemTable != nil {
		return utils.FindValueInStruct(c.Name, datarow.SystemTable)
	} else {
		return datarow.ProcessData(c.Order, c.Size, c.isStatic(),
			c.VarLenOrder-uint16(skippedVarCols), lobPages, textLOBPages, fixColsOffset)
	}

}

func (table Table) getHeader() utils.Record {
	var record utils.Record
	for _, c := range table.Schema {
		record = append(record, c.Name)
	}
	return record
}

func (c Column) Print(data []byte) {
	if c.Type == "varchar" || c.Type == "text" || c.Type == "ntext" {
		fmt.Printf("%s = %s LEN %d ", c.Name, string(data), len(data))
	} else if c.Type == "int" || c.Type == "tinyint" || c.Type == "bigint" {
		fmt.Printf("%s = %d ", c.Name, utils.ToInt32(data))
	}
}

func (table *Table) addColumn(name string, coltype string, size uint16, order uint16) []Column {
	col := Column{Name: name, Type: coltype, Size: size, Order: order}
	table.Schema = append(table.Schema, col)
	return table.Schema
}

func (table *Table) addColumns(results []page.Result[string, string, uint16, uint16]) []Column {
	var columns []Column
	for _, res := range results {
		columns = table.addColumn(res.First, res.Second, res.Third, res.Fourth)
	}
	return columns
}

func (table Table) printSchema() {
	if table.Schema != nil {

		fmt.Printf("Schema:  %s \n", table.Name)
		for _, col := range table.Schema {
			fmt.Printf(" | %s %s ", col.Name, col.Type)
		}
		fmt.Printf("\n")
	}

}

func (table Table) printAllocation(pageIds map[uint32]string) {
	fmt.Printf("objectID %d  PartitionId %d \n pages Id \n",
		table.ObjectId, table.Rowsetid)
	for pageId, pageType := range pageIds {
		fmt.Printf(" %d %s \n", pageId, pageType)
	}

}

func (table Table) GetRecords() utils.Records {
	var records utils.Records
	records = append(records, table.getHeader())
	for _, row := range table.rows {
		var record utils.Record
		for _, c := range table.Schema {
			record = append(record, c.toString(row[c.Name]))

		}
		records = append(records, record)
	}
	return records

}

func (table Table) printData() {
	for _, row := range table.rows {
		fmt.Printf("\n---------------------------\n")
		for _, c := range table.Schema {
			c.Print(row[c.Name])

		}

	}
	fmt.Printf("\n")
}

func (table *Table) setContent(dataPages page.PageMap,
	lobPages page.PageMap, textLobPages page.PageMap) {
	forwardPages := map[uint32][]uint32{} //list by when seen forward pointer with parent page
	var rows []ColMap
	fmt.Printf("reconstructing table %s\n", table.Name)
	for _, page := range dataPages {
		if page.HasForwardingPointers() {
			forwardPages[page.Header.PageId] = page.FollowForwardingPointers()

		}

		for _, datarow := range page.DataRows {
			m := make(ColMap)
			skippedVarCols := 0 // counts skipped var cols
			nofCols := len(table.Schema)

			if int(datarow.NumberOfCols) != nofCols { // mismatch data page and table schema!
				continue
			}
			fixColsOffset := 0
			for _, col := range table.Schema {

				if utils.HasFlagSet(datarow.NullBitmap, int(col.Order)-1, nofCols) { //col is NULL skip when ASCII 49  (1)
					if !col.isStatic() {
						skippedVarCols++
					}

					continue
				}

				m[col.Name] = col.addContent(datarow, skippedVarCols, lobPages, textLobPages, fixColsOffset)

				if col.isStatic() {

					fixColsOffset += int(col.Size)

				}
			}
			rows = append(rows, m)

		}
	}

	table.rows = rows

}
