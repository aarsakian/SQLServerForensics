package db

import (
	mslogger "MSSQLParser/logger"
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

type SqlVariant struct {
	BaseType   uint8
	Version    uint8
	Properties *SqlVariantProperties
	Value      []byte
}

type SqlVariantProperties struct {
	Precision     uint8
	Scale         uint8
	MaximumLength uint16
	CollationId   uint32
}

func (sqlVariant SqlVariant) getData() string {
	if sqlVariant.BaseType == 0x23 {
		return fmt.Sprintf("%d", utils.ToInt32(sqlVariant.Value))
	} else if sqlVariant.BaseType == 0x23 { //string
		return fmt.Sprintf("%s", sqlVariant.Value)
	}
	return ""
}

func (c Column) isStatic() bool {

	if c.Type == "varchar" || c.Type == "nvarchar" ||
		c.Type == "varbinary" || c.Type == "xml" || c.Type == "text" ||
		c.Type == "ntext" || c.Type == "image" || c.Type == "hierarchyid" ||
		c.Type == "float" || c.Type == "sql_variant" || c.Type == "sysname" {
		return false
	} else {
		return true
	}

}

func (c Column) parseSqlVariant(data []byte) SqlVariant {
	var sqlVariant *SqlVariant = new(SqlVariant)
	utils.Unmarshal(data, sqlVariant)
	var sqlVariantProperties SqlVariantProperties
	if sqlVariant.BaseType == 0x38 { //int
		sqlVariantProperties = SqlVariantProperties{Precision: data[2], Scale: data[3]}
		sqlVariant.Value = data[3:]
	} else if sqlVariant.BaseType == 0x23 { //string

		sqlVariantProperties = SqlVariantProperties{MaximumLength: utils.ToUint16(data[2:4]),
			CollationId: utils.ToUint32(data[4:8])}
		sqlVariant.Value = data[8:]
	}
	sqlVariant.Properties = &sqlVariantProperties
	return *sqlVariant
}

func (c Column) toString(data []byte) string {

	if c.Type == "varchar" || c.Type == "text" || c.Type == "ntext" {
		return fmt.Sprintf("%s", data)
	} else if c.Type == "int" {
		return fmt.Sprintf("%d", utils.ToInt32(data))
	} else if c.Type == "tinyint" {
		return fmt.Sprintf("%d", utils.ToInt8(data))
	} else if c.Type == "bigint" {
		return fmt.Sprintf("%d", utils.ToInt64(data))
	} else if c.Type == "varbinary" {
		return fmt.Sprintf("%x", data)
	} else if c.Type == "sql_variant" {
		sqlVariant := c.parseSqlVariant(data)
		return sqlVariant.getData()
	} else {
		return ""
	}
}

func (c *Column) addContent(datarow page.DataRow,
	lobPages page.PageMap, textLOBPages page.PageMap, fixColsOffset int) []byte {
	if datarow.SystemTable != nil {
		return utils.FindValueInStruct(c.Name, datarow.SystemTable)
	} else {
		return datarow.ProcessData(c.Order, c.Size, c.isStatic(),
			c.VarLenOrder, lobPages, textLOBPages, fixColsOffset)
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
	if c.Type == "varchar" || c.Type == "text" || c.Type == "ntext" ||
		c.Type == "varbinary" {
		fmt.Printf("%s = %s LEN %d ", c.Name, c.toString(data), len(data))
	} else {
		fmt.Printf("%s = %s ", c.Name, c.toString(data))
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
	for pageId, page := range dataPages {
		if page.HasForwardingPointers() {
			forwardPages[page.Header.PageId] = page.FollowForwardingPointers()

		}

		for did, datarow := range page.DataRows {
			m := make(ColMap)

			nofCols := len(table.Schema)

			if int(datarow.NumberOfCols) != nofCols { // mismatch data page and table schema!
				msg := fmt.Sprintf("Mismatch in number of cols in row %d, cols %d page %d and schema cols %d\n",
					did, int(datarow.NumberOfCols), pageId, nofCols)
				mslogger.Mslogger.Warning(msg)
				continue
			}
			if datarow.VarLenCols != nil && int(datarow.NumberOfVarLengthCols) != len(*datarow.VarLenCols) {
				msg := fmt.Sprintf("Mismatch in var cols! Investigate page %d row %d. Declaring %d in reality %d\n",
					pageId, did, int(datarow.NumberOfVarLengthCols), len(*datarow.VarLenCols))
				mslogger.Mslogger.Warning(msg)
				continue
			}
			fixColsOffset := 0
			for _, col := range table.Schema {

				if utils.HasFlagSet(datarow.NullBitmap, int(col.Order), nofCols) { //col is NULL skip when ASCII 49  (1)
					if !col.isStatic() {

					}
					msg := fmt.Sprintf(" %s SKIPPED  %d  type %s ", col.Name, col.Order, col.Type)
					mslogger.Mslogger.Error(msg)
					continue
				}

				//	fmt.Println(pageId, did, col.Name, col.isStatic(), col.Order, col.Type)
				m[col.Name] = col.addContent(datarow, lobPages, textLobPages, fixColsOffset)

				if col.isStatic() {

					fixColsOffset += int(col.Size)

				}

			}
			rows = append(rows, m)

		}
	}

	table.rows = rows

}
