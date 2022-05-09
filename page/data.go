package page

import (
	"fmt"
	"reflect"
	"strings"
)

type DataCol struct {
	id      uint16
	offset  uint16
	content []byte
}

type DataCols []DataCol

var DataRecord = map[uint8]string{0: "Primary", 1: "Forwarded", 2: "Forwarded Stub",
	3: "Index", 4: "Blob", 5: "Ghost Index",
	6: "Ghost Data"}

type DataRow struct {
	StatusA               uint8  //1
	StatusB               uint8  //2
	NofColsOffset         uint16 //3-4
	FixedLenCols          []byte
	NumberOfCols          uint16
	NullBitmap            uint16
	NumberOfVarLengthCols uint16
	DataCols              *DataCols
}

func (dataRow DataRow) GetFlags() string {
	recordType := DataRecord[dataRow.StatusA&6]
	nullBitmap := (map[bool]string{true: "NULL Bitmap"})[dataRow.StatusA&8 == 8]
	varLenCols := (map[bool]string{true: "Var length Cols"})[dataRow.StatusA&10 == 10]
	return strings.Join([]string{recordType, nullBitmap, varLenCols}, " ")
}

func (dataRow DataRow) Len() uint16 {
	return uint16(reflect.ValueOf(dataRow.FixedLenCols).Len() + 9)
}

func (dataRow DataRow) ShowData() {
	for _, dataCol := range *dataRow.DataCols {
		fmt.Printf("col id %d offset %x len %d \n",
			dataCol.id, dataCol.offset, reflect.ValueOf(dataCol.content).Len())
	}
}
