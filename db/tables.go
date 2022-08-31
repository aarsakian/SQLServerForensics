package db

import (
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"fmt"
)

type ColMap map[string][]byte

type Table struct {
	Name         string
	ObjectId     int32
	rows         []ColMap
	PartitionId  uint64
	PageObjectId int32
	Schema       []Column
	VarLenCols   []int
}

type Column struct {
	Name        string
	Type        string
	Size        uint16
	Order       uint16
	VarLenOrder uint16
}

func (c Column) isStatic() bool {

	if c.Type == "varchar" || c.Type == "nvarchar" ||
		c.Type == "varbinary" || c.Type == "xml" {
		return false
	} else {
		return true
	}

}

func (c *Column) addContent(datarow page.DataRow) []byte {

	return datarow.ProcessData(c.Order, c.Size, c.isStatic(), c.VarLenOrder)

}

func (c Column) Print(content []byte) {
	if c.Type == "varchar" {
		fmt.Printf("%s = %s ", c.Name, string(content))
	} else if c.Type == "int" || c.Type == "tinyint" || c.Type == "bigint" {
		fmt.Printf("%s = %d ", c.Name, utils.ToInt32(content))
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
		fmt.Printf("\n table %s objectID %d  PartitionId %d page Object Id %d \n", table.Name,
			table.ObjectId, table.PartitionId, table.PageObjectId)
		for _, col := range table.Schema {
			fmt.Printf("Schema %s %s ", col.Name, col.Type)
		}
	}

}

func (table Table) printData() {
	for _, row := range table.rows {
		fmt.Printf("\n---------------------------\n")
		for _, c := range table.Schema {
			c.Print(row[c.Name])

		}

	}
}

func (table *Table) setContent(tablePages []page.Page) {

	for _, page := range tablePages {

		if page.GetType() != "DATA" {
			continue
		}

		var rows []ColMap
		for _, datarow := range page.DataRows {
			m := make(ColMap)
			for _, col := range table.Schema {

				m[col.Name] = col.addContent(datarow)

			}
			rows = append(rows, m)

		}

		table.rows = rows

	}

}
