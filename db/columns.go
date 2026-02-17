package db

import (
	"MSSQLParser/data"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	b64 "encoding/base64"
	"fmt"

	"golang.org/x/text/encoding/charmap"
)

type ColData struct {
	Content       []byte
	LoggedColData *ColData
}

type Computed struct {
	Definition string
}

type ColMap map[string]ColData //name->coldata

type Column struct {
	Name         string
	Type         string
	Size         int16
	Order        uint16
	VarLenOrder  uint16
	CollationId  uint32
	Precision    uint8
	Scale        uint8
	Charmap      *charmap.Charmap
	CodePage     string
	OffsetMap    map[uint64]int16 //partitionId -> offset
	IslNullable  bool
	IsAnsiPadded bool
	IsIdentity   bool
	IsRowGUIDCol bool
	IsComputed   bool
	IsPersisted  bool
	IsColumnSet  bool
	IsFilestream bool
	Computed     *Computed
}

func (c Column) Print(data []byte) {

	fmt.Printf("%s ", c.toString(data))

}

func (computed Computed) Print() {
	fmt.Printf("%s", computed.Definition)
}

func (c Column) parseDecimal(data []byte) string {
	return utils.DecimalToStr(data, c.Precision, c.Scale)

}

func (c Column) parseReal(data []byte) string {
	return utils.RealToStr(data, c.Precision, c.Scale)

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

func (c Column) toString(data []byte) string {
	if len(data) == 0 {
		//mslogger.Mslogger.Warning(fmt.Sprintf("Empty data col %s", c.Name))
		return ""
	}
	//always defines number of bytes n never defines number of characters stored
	//<2019 sql server versions save in cp codepages
	switch c.Type {
	case "varchar", "text", "char":
		return utils.Decode(data, c.Charmap, c.CodePage)

	case "nvarchar", "ntext", "nchar": //n = number of byte pairs (10=10x2 20bytes in Latin1_Gen.... SC_UTF8)
		return utils.DecodeUTF16(data)
	case "datetime2":
		return utils.DateTime2Tostr(data)
	case "datetime":
		return utils.DateTimeTostr(data)
	case "int":
		return fmt.Sprintf("%d", utils.ToInt32(data))
	case "smallint":
		return fmt.Sprintf("%d", utils.ToInt16(data))
	case "tinyint":
		return fmt.Sprintf("%d", utils.ToInt8(data))
	case "bigint":
		return fmt.Sprintf("%d", utils.ToInt64(data))
	case "varbinary":
		return fmt.Sprintf("%x", data)
	case "decimal", "numeric": //synonyms
		return c.parseDecimal(data)
	case "sql_variant":
		sqlvariant := new(utils.SqlVariant)
		sqlvariant.Parse(data)
		if len(sqlvariant.Value) == 0 {
			return ""
		}
		return sqlvariant.GetData()
	case "image":
		return b64.StdEncoding.EncodeToString(data)
	case "bit":
		return utils.BitToString(data, 1) // less than 8 cols one byte required > 2 two bytes
	case "uniqueidentifier":
		return fmt.Sprintf("%x-%x-%x-%x-%x", utils.Reverse(data[0:4]), utils.Reverse(data[4:6]),
			utils.Reverse(data[6:8]), data[8:10], data[10:16])
	case "money":
		return utils.MoneyToStr(data)
	case "date":
		return utils.CheckLenBefore(data, utils.DateToStr)
	case "float":
		return utils.CheckLenBefore(data, utils.FloatToStr)
	case "real":
		return c.parseReal(data)
	case "smalldatetime":
		return utils.ParseSmallDateTime(data)
	case "hierarchyid":
		return fmt.Sprintf("%x", data)
	case "time":
		return c.ParseTime(data)

	default:
		mslogger.Mslogger.Warning(fmt.Sprintf("col %s type %s not yet implemented", c.Name, c.Type))
		return fmt.Sprintf("unhandled type %s", c.Type)
	}
}

func (c Column) Parse(data []byte) interface{} {
	switch c.Type {
	case "int":
		return utils.ToInt32(data)
	case "smallint":
		return utils.ToInt16(data)
	case "tinyint":
		return utils.ToInt8(data)
	default:
		return nil
	}
}

func (c Column) ParseTime(data []byte) string {
	return utils.ParseTime(data, int(c.Precision))
}

func (c *Column) addContent(datarow data.DataRow,
	lobPages page.PagesPerId[uint32], textLOBPages page.PagesPerId[uint32], partitionId uint64, nofNullCols int) ([]byte, error) {
	if datarow.SystemTable != nil {
		return utils.FindValueInStruct(c.Name, datarow.SystemTable), nil
	} else {

		if !c.isStatic() && datarow.HasBlobInfo(c.VarLenOrder-uint16(nofNullCols)) {
			rowIds, textTimestamp := datarow.GetBloBInfo(c.VarLenOrder - uint16(nofNullCols))
			if !lobPages.IsEmpty() && len(rowIds) != 0 { //only when there are lobpages proceed
				var content []byte
				for _, rowId := range rowIds {
					lobPage := lobPages.GetFirstPage(rowId.PageId)
					content = append(content,
						lobPage.GetLobData(lobPages, textLOBPages,
							uint(rowId.SlotNumber), uint(textTimestamp))...)
				}
				return content, nil
			} else {
				return nil, fmt.Errorf("lob data not found for col %s", c.Name)
			}
		} else {
			return datarow.ProcessData(c.Order, c.Size, c.OffsetMap[partitionId], c.isStatic(),
				c.VarLenOrder-uint16(nofNullCols))
		}
	}

}
