package page

import (
	"MSSQLParser/utils"
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

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

type DataCols []DataCol

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
	return dataRow.StatusA&16 == 16
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
	for _, dataCol := range *dataRow.VarLenCols {
		fmt.Printf("col id %d offset %x len %d \n",
			dataCol.id, dataCol.offset, reflect.ValueOf(dataCol.content).Len())
	}
}
