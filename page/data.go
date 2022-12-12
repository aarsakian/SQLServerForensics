package page

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

type DataCol struct {
	id      int
	offset  uint16
	content []byte
}

type DataRows []DataRow

type RowIds []RowId

type DataCols []DataCol

var DataRecord = map[uint8]string{0: "Primary", 1: "Forwarded", 2: "Forwarded Stub",
	3: "Index", 4: "Blob", 5: "Ghost Index",
	6: "Ghost Data"}

type RowId struct {
	Length     uint32
	PageId     uint32
	FileId     uint16
	SlotNumber uint16
}

type InlineBLob struct {
	Type       uint8
	Link       uint8
	IndexLevel uint8
	Unused     byte
	UpdateSeq  uint32
	Timestamp  uint32
	RowIds     RowIds
}

type DataRow struct {
	// min len 9 bytes
	StatusA               uint8  //1
	StatusB               uint8  //1
	NofColsOffset         uint16 //2
	FixedLenCols          []byte //1-
	NumberOfCols          uint16 //2
	NullBitmap            []byte //1-
	NumberOfVarLengthCols uint16 //0-
	VarLengthColOffsets   []uint16
	VarLenCols            *DataCols
	InlineBLob            *InlineBLob
	SystemTable           SystemTable
}

func (dataRow DataRow) GetFlags() string {
	recordType := DataRecord[dataRow.StatusA&6]
	nullBitmap := (map[bool]string{true: "NULL Bitmap"})[dataRow.StatusA&8 == 8]
	varLenCols := (map[bool]string{true: "Var length Cols"})[dataRow.StatusA&10 == 10]
	return strings.Join([]string{recordType, nullBitmap, varLenCols}, " ")
}

func (dataRow DataRow) GetVarCalOffset() uint16 {

	return dataRow.NofColsOffset + uint16(unsafe.Sizeof(dataRow.NumberOfCols)) +
		uint16(reflect.ValueOf(dataRow.NullBitmap).Len()) +
		uint16(unsafe.Sizeof(dataRow.NumberOfVarLengthCols)) +
		uint16(reflect.ValueOf(dataRow.VarLengthColOffsets).Len()*2)
}

func (dataRow DataRow) ShowData() {
	for _, dataCol := range *dataRow.VarLenCols {
		fmt.Printf("col id %d offset %x len %d \n",
			dataCol.id, dataCol.offset, reflect.ValueOf(dataCol.content).Len())
	}
}
