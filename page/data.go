package page

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

//statusA structure 1-3 bits = page type, 5 = hasnullbitmap, 6=hasvarlencols, 7=version tag?

type ForwardingPointers []ForwardingPointer

type ForwardingPointer struct { //smallest size of data row structure appear when data that was placed does not fit anymore
	Header uint8
	RowId  utils.RowId
}

type DataCol struct {
	id           int
	offset       uint16
	content      []byte
	InlineBlob24 *InlineBLob24
	InlineBlob16 *InlineBLob16
}

type DataRows []DataRow

type RowIds []utils.RowId

type DataCols []DataCol //holds varying len cols

var DataRecord = map[uint8]string{
	0: "Primary Record", 2: "Forwarded Record", 4: "Forwarding Record", 6: "Index Record",
	8: "BLOB Fragment", 10: "Ghost Index Record", 12: "Ghost Data Record", 14: "Ghost Version Record",
}

type InlineBLob24 struct {
	Type       uint8
	Link       uint8
	IndexLevel uint8
	Unused     byte
	UpdateSeq  uint32
	Timestamp  uint32
	Length     uint32
	RowId      utils.RowId //12-
}

type InlineBLob16 struct { //points to text lob
	Timestamp uint32
	Length    uint32
	RowId     utils.RowId //4-
}

type DataRow struct { // max size is 8060 bytes  min record header 7 bytes
	// min len 9 bytes
	StatusA               uint8  //1-2
	StatusB               uint8  //2-3
	NofColsOffset         uint16 //3-5
	FixedLenCols          []byte //0-
	NumberOfCols          uint16 //5-6
	NullBitmap            []byte //6-7
	NumberOfVarLengthCols uint16 //0-
	VarLengthColOffsets   []int16
	VarLenCols            *DataCols
	SystemTable           SystemTable
}

func GetRowType(statusA byte) string {

	for flagbyte, flagname := range DataRecord {
		if flagbyte == 0 {
			continue //cannot compare with zero bitmask
		}
		if statusA&flagbyte == flagbyte {
			return flagname

		}
	}
	return DataRecord[0] // Primary Record
}

func (dataRow DataRow) GetFlags() string {
	recordType := DataRecord[dataRow.StatusA&14]
	nullBitmap := (map[bool]string{true: "NULL Bitmap"})[dataRow.HasNullBitmap()]
	varLenCols := (map[bool]string{true: "Var length Cols"})[dataRow.HasVarLenCols()]
	return strings.Join([]string{recordType, nullBitmap, varLenCols}, " ")
}

func (dataRow DataRow) HasNullBitmap() bool {
	return utils.HasNullBitmap(dataRow.StatusA)
}

func (dataRow DataRow) HasVersionTag() bool {
	return dataRow.StatusA&32 == 32
}

func (dataRow DataRow) HasVarLenCols() bool {
	return utils.HasVarLengthCols(dataRow.StatusA)
}

func (dataCol DataCol) hasBlob24() bool {
	return dataCol.InlineBlob24 != nil

}

func (dataCol DataCol) hasBlob16() bool {
	return dataCol.InlineBlob16 != nil

}

func (dataCol DataCol) GetLOBPage() uint32 {
	if dataCol.hasBlob16() {
		return dataCol.InlineBlob16.RowId.PageId // needs check for more rowids
	} else if dataCol.hasBlob24() {
		return dataCol.InlineBlob24.RowId.PageId // needs check for more rowids
	}
	return 0
}

func (dataRow DataRow) GetBloBPageId(colNum uint16) uint32 {
	return (*dataRow.VarLenCols)[colNum].GetLOBPage()
}

func (dataRow DataRow) GetVarCalOffset() int16 { // start offset for var col len

	return int16(dataRow.NofColsOffset) + int16(unsafe.Sizeof(dataRow.NumberOfCols)) +
		int16(reflect.ValueOf(dataRow.NullBitmap).Len()) +
		int16(unsafe.Sizeof(dataRow.NumberOfVarLengthCols)) +
		int16(reflect.ValueOf(dataRow.VarLengthColOffsets).Len()*2)
}

func (dataRow DataRow) ShowData() {
	if dataRow.SystemTable != nil {
		dataRow.SystemTable.ShowData()
	}
	fmt.Printf("static ends %d %x ", 4+reflect.ValueOf(dataRow.FixedLenCols).Len(), dataRow.FixedLenCols)
	for _, dataCol := range *dataRow.VarLenCols {
		fmt.Printf(" %d  %d  %x ",
			dataCol.offset, reflect.ValueOf(dataCol.content).Len(), dataCol.content)
	}
	fmt.Printf("\n")
}

func (dataRow *DataRow) ProcessVaryingCols(data []byte, offset int) { // data per slot
	var datacols DataCols
	var inlineBlob24 *InlineBLob24
	var inlineBlob16 *InlineBLob16
	startVarColOffset := dataRow.GetVarCalOffset()
	for idx, endVarColOffset := range dataRow.VarLengthColOffsets {
		msg := fmt.Sprintf("%d var col at %d", idx, offset+int(startVarColOffset))
		mslogger.Mslogger.Info(msg)

		if endVarColOffset < 0 {
			endVarColOffset = utils.RemoveSignBit(endVarColOffset)
		}

		if endVarColOffset < startVarColOffset {
			continue
		} else if int(startVarColOffset) > len(data) {
			break
		} else if int(endVarColOffset) > len(data) {
			endVarColOffset = int16(len(data))
		} else if int(endVarColOffset) > 8192-2*len(dataRow.VarLengthColOffsets) { //8192 - 2 for each slot
			endVarColOffset = int16(8192 - 2*len(dataRow.VarLengthColOffsets))
		}
		cpy := make([]byte, endVarColOffset-startVarColOffset) // var col length
		copy(cpy, data[startVarColOffset:endVarColOffset])
		startVarColOffset = endVarColOffset

		var rowId *utils.RowId = new(utils.RowId)
		if len(cpy) == 24 { // only way to guess that we have a row overflow data
			inlineBlob24 = new(InlineBLob24)
			utils.Unmarshal(cpy, inlineBlob24)
			utils.Unmarshal(cpy[16:], rowId)
			inlineBlob24.RowId = *rowId

		} else if len(cpy) == 16 {
			inlineBlob16 = new(InlineBLob16)
			utils.Unmarshal(cpy, inlineBlob16)
			utils.Unmarshal(cpy[8:], rowId)
			inlineBlob16.RowId = *rowId
		}

		if dataRow.SystemTable != nil {
			dataRow.SystemTable.SetName(cpy)
		} else if inlineBlob16 != nil {
			datacols = append(datacols,
				DataCol{id: idx, content: cpy, offset: uint16(startVarColOffset), InlineBlob16: inlineBlob16})
		} else if inlineBlob24 != nil {
			datacols = append(datacols,
				DataCol{id: idx, content: cpy, offset: uint16(startVarColOffset), InlineBlob24: inlineBlob24})

		} else {
			datacols = append(datacols,
				DataCol{id: idx, content: cpy, offset: uint16(startVarColOffset)})
		}

	}

	dataRow.VarLenCols = &datacols

}

func (dataRow *DataRow) ProcessData(colId uint16, colsize int16, startoffset int16,
	static bool, valorder uint16, lobPages PageMapIds, textLobPages PageMapIds) (data []byte) {

	if static {
		fixedLenColsOffset := 4 // include status flag nofcols
		if int(colsize) > len(dataRow.FixedLenCols) {
			mslogger.Mslogger.Error(fmt.Sprintf("Col Id %d Column size %d exceeded fixed len cols size %d",
				colId, colsize, len(dataRow.FixedLenCols)))
			return nil // bad practice ???
		} else if int(startoffset) > len(dataRow.FixedLenCols)+fixedLenColsOffset {
			mslogger.Mslogger.Error(fmt.Sprintf("Col Id %d column start offset %d exceeded available area of fixed len cols by %d",
				colId, startoffset, int(startoffset)-len(dataRow.FixedLenCols)))
			return nil
		} else if int(startoffset)+int(colsize) > len(dataRow.FixedLenCols)+fixedLenColsOffset {
			mslogger.Mslogger.Error(fmt.Sprintf("Col Id %d End offset %d exceeded available area of fixed len cols by %d ?",
				colId, int(startoffset)+int(colsize), int(startoffset)+int(colsize)-len(dataRow.FixedLenCols)))
			return nil
		} else if startoffset < 4 {
			mslogger.Mslogger.Error(fmt.Sprintf("Col id %d start offset %d is less than 4 fixed len cols offset",
				colId, startoffset))
			return nil
		} else {
			return dataRow.FixedLenCols[startoffset-int16(fixedLenColsOffset) : startoffset+colsize-int16(fixedLenColsOffset)] //offset is from start of datarow
		}

	} else {
		if dataRow.NumberOfVarLengthCols == 0 || dataRow.NumberOfVarLengthCols <= valorder {
			// should had bitmap set to 1 however it is not expiremental
			return nil
		}
		pageId := dataRow.GetBloBPageId(valorder)
		if pageId != 0 {

			lobPage := lobPages[pageId]

			return lobPage.LOBS.GetData(lobPages, textLobPages) // might change
		} else {
			content := (*dataRow.VarLenCols)[valorder].content
			if len(content) > int(colsize) {
				mslogger.Mslogger.Error(fmt.Sprintf("Col id %d data len %d truncated to col size %d", colId, len(content), colsize))
				content = content[:colsize]

			}
			return content
		}

	}

}

func (dataRow *DataRow) Process(systemtable SystemTable) {

	utils.Unmarshal(dataRow.FixedLenCols, systemtable)
	dataRow.SystemTable = systemtable

}
