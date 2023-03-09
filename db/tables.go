package db

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"fmt"
	"sort"
)

type ColMap map[string][]byte

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
}

type Column struct {
	Name        string
	Type        string
	Size        int16
	Order       uint16
	VarLenOrder uint16
	CollationId uint32
	Precision   uint8
	Scale       uint8
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

func (c Column) parseDecimal(data []byte) string {
	return utils.DecimalToStr(data, c.Precision, c.Scale)

}

func (sqlVariant SqlVariant) getData() string {
	if sqlVariant.BaseType == 0x23 {
		return fmt.Sprintf("%d", utils.ToInt32(sqlVariant.Value))
	} else if sqlVariant.BaseType == 0x7f {
		return fmt.Sprintf("%d", utils.ToInt64(sqlVariant.Value))
	} else if sqlVariant.BaseType == 0xad { //string
		return fmt.Sprintf("%x", sqlVariant.Value)
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
	if len(data) == 0 {
		mslogger.Mslogger.Warning(fmt.Sprintf("Empty data col %s", c.Name))
		return ""
	}
	if c.Type == "varchar" || c.Type == "nvarchar" || c.Type == "text" || c.Type == "ntext" {
		if c.CollationId == 872468488 { //SQL_Latin1_General_CP1_CI_AS
			return string(data)
		} else {
			return utils.DecodeUTF16(data)
		}

	} else if c.Type == "datetime2" {
		return utils.DateTime2Tostr(data)
	} else if c.Type == "datetime" {
		return utils.DateTimeTostr(data)
	} else if c.Type == "int" {
		return fmt.Sprintf("%d", utils.ToInt32(data))
	} else if c.Type == "smallint" {
		return fmt.Sprintf("%d", utils.ToInt16(data))
	} else if c.Type == "tinyint" {
		return fmt.Sprintf("%d", utils.ToInt8(data))
	} else if c.Type == "bigint" {
		return fmt.Sprintf("%d", utils.ToInt64(data))
	} else if c.Type == "varbinary" {
		return fmt.Sprintf("%x", data)
	} else if c.Type == "decimal" {

		return c.parseDecimal(data)
	} else if c.Type == "sql_variant" {
		sqlVariant := c.parseSqlVariant(data)
		return sqlVariant.getData()
	} else if c.Type == "bit" {
		return fmt.Sprintf("%1d", utils.ToInt8(data))
	} else if c.Type == "uniqueidentifier" {
		return fmt.Sprintf("%x-%x-%x-%x-%x", utils.Reverse(data[0:4]), utils.Reverse(data[4:6]),
			utils.Reverse(data[6:8]), data[8:10], data[10:16])
	} else {
		mslogger.Mslogger.Warning(fmt.Sprintf("col %s type %s not yet implemented", c.Name, c.Type))
		return fmt.Sprintf("unhandled type %s %x", c.Type, data[:])
	}
}

func (c *Column) addContent(datarow page.DataRow,
	lobPages page.PageMapIds, textLOBPages page.PageMapIds, fixColsOffset int) []byte {
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

	fmt.Printf("%s ", c.toString(data))

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
	columns := make([]Column, len(table.Schema))
	vid := 0
	// range copies values
	for idx := range table.Schema {
		if columns[idx].isStatic() {
			columns[idx].VarLenOrder = 0
		} else {

			columns[idx].VarLenOrder = uint16(vid)
			vid++
		}

	}
}

func (table Table) printSchema() {
	if table.Schema != nil {

		fmt.Printf("Schema:  %s \n", table.Name)
		for _, col := range table.Schema {
			fmt.Printf(" | %s %s", col.Name, col.Type)
		}
		fmt.Printf("\nDynamic cols")
		for _, col := range table.Schema {
			if col.isStatic() {
				continue
			}
			fmt.Printf(" %s", col.Name)
		}
		fmt.Printf("\n")
	}

}

func (table Table) printAllocation() {
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
			record = append(record, c.toString(row[c.Name]))

		}
		records = append(records, record)
	}
	return records

}

func (table Table) printHeader() {
	fmt.Printf("\n---------------------------\n")
	for _, c := range table.Schema {
		fmt.Printf("%s ", c.Name)
	}
	fmt.Printf("\n")
}
func (table Table) printData() {
	for _, row := range table.rows {

		for _, c := range table.Schema {
			c.Print(row[c.Name])

		}

	}
	fmt.Printf("\n")
}

func (table *Table) setContent(dataPages page.PageMapIds,
	lobPages page.PageMapIds, textLobPages page.PageMapIds) {
	forwardPages := map[uint32][]uint32{} //list by when seen forward pointer with parent page
	var rows []ColMap

	for pageId, page := range dataPages {
		if page.HasForwardingPointers() {
			forwardPages[page.Header.PageId] = page.FollowForwardingPointers()

		}

		for did, datarow := range page.DataRows {
			m := make(ColMap)

			nofCols := len(table.Schema)

			if int(datarow.NumberOfCols) != nofCols { // mismatch data page and table schema!
				msg := fmt.Sprintf("Mismatch in number of cols %d in row %d,  page %d and schema cols %d table %s",
					int(datarow.NumberOfCols), did, pageId, nofCols, table.Name)
				mslogger.Mslogger.Warning(msg)
				continue
			}
			if datarow.VarLenCols != nil && int(datarow.NumberOfVarLengthCols) != len(*datarow.VarLenCols) {
				msg := fmt.Sprintf("Mismatch in var cols! Investigate page %d row %d. Declaring %d in reality %d table %s",
					pageId, did, int(datarow.NumberOfVarLengthCols), len(*datarow.VarLenCols), table.Name)
				mslogger.Mslogger.Warning(msg)
				continue
			}
			fixColsOffset := 0
			for colnum, col := range table.Schema {
				//schema is sorted by colorder use colnum instead of col.Order
				if colnum+1 != int(col.Order) {
					mslogger.Mslogger.Info(fmt.Sprintf("Discrepancy possible column %s deletion %d order %d !", col.Name, colnum+1, col.Order))
				}
				if utils.HasFlagSet(datarow.NullBitmap, colnum+1, nofCols) { //col is NULL skip when ASCII 49  (1)

					//msg := fmt.Sprintf(" %s SKIPPED  %d  type %s ", col.Name, col.Order, col.Type)
					//mslogger.Mslogger.Error(msg)
					continue
				}

				//mslogger.Mslogger.Info(col.Name + " " + fmt.Sprintf("%s %d %s %d", col.isStatic(), col.Order, col.Type, col.Size))
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
