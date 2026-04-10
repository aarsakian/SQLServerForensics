package tables

import (
	"MSSQLParser/data"

	LDF "MSSQLParser/ldf"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"bytes"
	"fmt"
	"time"

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

type Row struct {
	ColMap            ColMap
	LoggedOperation   string
	Carved            bool
	Logged            bool
	LogDate           time.Time
	RowId             utils.RowId
	LSN               utils.LSN
	LogBasedLinkedRow *Row
}

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

func (row *Row) UpdateWithLogRecord(record *LDF.Record, pageFlushed bool, tableSchma []Column) {

	row.LoggedOperation = record.GetOperationType()
	switch row.LoggedOperation {
	case "LOP_INSERT_ROW":
		if pageFlushed {
			row.MarkInserted(record)

		}

	case "LOP_MODIFY_ROW":
		if !pageFlushed {

			row.MarkModified(record, tableSchma, false)
		}

	case "LOP_DELETE_ROW":
		if !pageFlushed {
			row.MarkDeleted(record)
		}
	}

}

func (row *Row) MarkDeleted(record *LDF.Record) {
	row.LoggedOperation = "Deleted at  " + record.GetBeginCommitDate() +
		fmt.Sprintf(" commited at %s", record.GetEndCommitDate())
	row.Logged = true
	row.LogDate = record.GetBeginCommitDateObj()
	row.LSN = record.CurrentLSN
}

func (row *Row) MarkInserted(record *LDF.Record) {
	row.Logged = true

	row.LogDate = record.GetBeginCommitDateObj()
	row.LSN = record.CurrentLSN
	row.LoggedOperation = "Inserted at  " + record.GetBeginCommitDate() +
		fmt.Sprintf(" commited at %s", record.GetEndCommitDate())
}

func (row *Row) MarkModified(record *LDF.Record, tableSchema []Column, carved bool) {
	row.Logged = true
	row.LogDate = record.GetBeginCommitDateObj()
	row.LSN = record.CurrentLSN
	row.LoggedOperation += "Pending flushing, Modified at " +
		record.GetBeginCommitDate() + fmt.Sprintf(" commited at %s", record.GetEndCommitDate())

	logBasedLinkedRow := new(Row)

	logBasedLinkedRow.LogDate = record.GetBeginCommitDateObj()
	logBasedLinkedRow.Carved = true
	logBasedLinkedRow.Logged = true

	for _, c := range tableSchema {
		if record.Lop_Insert_Delete != nil && c.OffsetMap[record.Lop_Insert_Delete.PartitionID] >= int16(record.Lop_Insert_Delete.OffsetInRow) {
			var newcontent bytes.Buffer
			newcontent.Grow(int(c.Size))

			colData := row.ColMap[c.Name]
			//new data from startoffset -> startoffset + modifysize
			startOffset := int16(record.Lop_Insert_Delete.OffsetInRow) - c.OffsetMap[record.Lop_Insert_Delete.PartitionID]
			if startOffset > 0 {
				newcontent.Write(colData.Content[:startOffset]) //unchanged content
				newcontent.Write(record.Lop_Insert_Delete.RowLogContents[0])
				newcontent.Write(colData.Content[startOffset+int16(record.Lop_Insert_Delete.ModifySize):])

				colData.LoggedColData = &ColData{Content: newcontent.Bytes()}
				logBasedLinkedRow.ColMap[c.Name] = colData

			}

			break
		}
	}

	row.LogBasedLinkedRow = logBasedLinkedRow

}

func (c Column) Print(data []byte) {

	fmt.Printf("%s ", c.ToString(data))

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

func (c Column) IsStatic() bool {

	if c.Type == "varchar" || c.Type == "nvarchar" ||
		c.Type == "varbinary" || c.Type == "xml" || c.Type == "text" ||
		c.Type == "ntext" || c.Type == "image" || c.Type == "hierarchyid" ||
		c.Type == "sql_variant" || c.Type == "sysname" {
		return false
	} else {
		return true
	}

}

func (c Column) ToString(data []byte) string {
	if len(data) == 0 {
		//mslogger.Mslogger.Warning(fmt.Sprintf("Empty data col %s", c.Name))
		return "NULL"
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
		return fmt.Sprintf("0x%x", data)
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
		return fmt.Sprintf("0x%x", data) //b64.StdEncoding.EncodeToString(data)
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

func (c *Column) AddContent(datarow data.DataRow,
	lobPages page.PagesPerId[uint32], textLOBPages page.PagesPerId[uint32], partitionId uint64) ([]byte, error) {

	if datarow.SystemTable != nil {
		return utils.FindValueInStruct(c.Name, datarow.SystemTable), nil
	} else {

		if !c.IsStatic() && datarow.HasBlobInfo(c.VarLenOrder) ||
			!c.IsStatic() && datarow.HasTextBlobInfo(c.VarLenOrder) {
			rowIds, textTimestamp := datarow.GetBloBInfo(c.VarLenOrder)
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

			return datarow.ProcessData(c.Order, c.Size, c.OffsetMap[partitionId], c.IsStatic(),
				c.VarLenOrder)

		}
	}

}
