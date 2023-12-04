package db

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	b64 "encoding/base64"
	"fmt"
)

type ColData struct {
	Content []byte
	Carved  bool
}

type ColMap map[string]ColData

type Column struct {
	Name        string
	Type        string
	Size        int16
	Order       uint16
	VarLenOrder uint16
	CollationId uint32
	Precision   uint8
	Scale       uint8
	Offset      int16
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

func (c Column) Print(data []byte) {

	fmt.Printf("%s ", c.toString(data))

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
		c.Type == "sql_variant" || c.Type == "sysname" {
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
	if c.Type == "varchar" || c.Type == "text" { //ansi
		if c.CollationId == 872468488 { //SQL_Latin1_General_CP1_CI_AS
			return string(data)
		} else if c.CollationId == 53255 { // Greek_CI_AS
			return utils.FromGreekCIToString(data)
		} else {
			return string(data)
		}

	} else if c.Type == "nvarchar" || c.Type == "ntext" { //n implies unicode
		return utils.DecodeUTF16(data)
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
	} else if c.Type == "image" {
		return b64.StdEncoding.EncodeToString(data)
	} else if c.Type == "bit" {
		return fmt.Sprintf("%1d", utils.ToInt8(data))
	} else if c.Type == "uniqueidentifier" {
		return fmt.Sprintf("%x-%x-%x-%x-%x", utils.Reverse(data[0:4]), utils.Reverse(data[4:6]),
			utils.Reverse(data[6:8]), data[8:10], data[10:16])
	} else if c.Type == "money" {
		return utils.MoneyToStr(data)
	} else if c.Type == "date" {
		return utils.DateToStr(data)
	} else if c.Type == "flaot" {
		return utils.FloatToStr(data)
	} else {
		mslogger.Mslogger.Warning(fmt.Sprintf("col %s type %s not yet implemented", c.Name, c.Type))
		return fmt.Sprintf("unhandled type %s", c.Type)
	}
}

func (c *Column) addContent(datarow page.DataRow,
	lobPages page.PageMapIds, textLOBPages page.PageMapIds) ([]byte, error) {
	if datarow.SystemTable != nil {
		return utils.FindValueInStruct(c.Name, datarow.SystemTable), nil
	} else {
		return datarow.ProcessData(c.Order, c.Size, c.Offset, c.isStatic(),
			c.VarLenOrder, lobPages, textLOBPages)
	}

}
