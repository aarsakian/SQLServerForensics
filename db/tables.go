package db

import "fmt"

type Table struct {
	Name    string
	Columns []Column
}

type Column struct {
	Name string
}

func (table *Table) addColumn(name string) []Column {
	col := Column{name}
	table.Columns = append(table.Columns, col)
	return table.Columns
}

func (table *Table) addColumns(names []string) []Column {
	var columns []Column
	for _, colname := range names {
		columns = table.addColumn(colname)
	}
	return columns
}

func (table Table) printCols() {
	fmt.Printf("\n table %s ", table.Name)
	for _, col := range table.Columns {
		fmt.Printf("%s ", col.Name)
	}

}
