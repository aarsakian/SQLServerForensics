package db

import (
	"MSSQLParser/page"
	"fmt"
)

type Table struct {
	Name         string
	ObjectId     int32
	Columns      []Column
	PartitionId  uint64
	PageObjectId int32
}

type Column struct {
	Name  string
	Type  string
	Size  uint16
	Order uint16
	data  []byte
}

func (c Column) isStatic() bool {

	if c.Type == "varchar" || c.Type == "nvarchar" ||
		c.Type == "varbinary" || c.Type == "xml" {
		return false
	} else {
		return true
	}

}

func (c *Column) addContent(datarows page.DataRows) {
	for _, datarow := range datarows {
		if c.data != nil { //arleady set skip
			continue
		}
		c.data = datarow.ProcessData(c.Order, c.Size, c.isStatic())

		fmt.Printf("%s %t %d %x \n",
			c.Name,
			c.isStatic(), c.Size, c.data)
	}
}

func (table *Table) addColumn(name string, coltype string, size uint16, order uint16) []Column {
	col := Column{Name: name, Type: coltype, Size: size}
	table.Columns = append(table.Columns, col)
	return table.Columns
}

func (table *Table) addColumns(results []page.Result[string, string, uint16, uint16]) []Column {
	var columns []Column
	for _, res := range results {
		columns = table.addColumn(res.First, res.Second, res.Third, res.Fourth)
	}
	return columns
}

func (table Table) printCols() {
	if table.Columns != nil {
		fmt.Printf("\n table %s objectID %d  PartitionId %d page Object Id %d \n", table.Name,
			table.ObjectId, table.PartitionId, table.PageObjectId)
		for _, col := range table.Columns {
			fmt.Printf("%s %s ", col.Name, col.Type)
		}
	}

}

func (table Table) getContent(tablePages []page.Page) {

	for _, page := range tablePages {
		if page.GetType() != "DATA" {
			continue
		}
		if table.Name != "DataRows" {
			continue
		}
		fmt.Printf("\n table name %s %d \n", table.Name, page.Header.PageId)
		//for _, col := range table.Columns {
		for _, col := range table.Columns {
			col.addContent(page.DataRows)
		}

	}

}
