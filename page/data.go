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

type Auid struct {
	UniqueId uint16
	ObjectId uint32
	zeros    uint32
}

type SysAllocUints struct {
	Auid       Auid //64
	Type       uint8
	OwnerId    uint64
	Fgid       uint32
	PgFirst    uint64 //6 bytes
	Pgroot     uint64 //6 bytes
	PgFirstIAM uint64 //6
	PgUsed     uint64
	PgData     uint64
	PgReserved uint64
	DbFragId   uint32
}

type SysColpars struct {
	//objectId == 41
	unknown  [4]byte
	ObjectId uint32 //4 -8
	Number   uint16 //8-10
	ColId    uint32 //10 -12
	Colorder uint16 //12 - 14
	Xtype    uint8  // 14 sys.sysscalartypes.xtype.
	Utype    uint32 //15-19 sys.sysscalartypes.id
	Colsize  int16  //19-21
	unknown4 [30]byte
	Length   int16 // 51-53  -1 = contains varlen types
	Name     string
}

type DataRows []interface {
	ShowData()
}

type DataCols []DataCol

var DataRecord = map[uint8]string{0: "Primary", 1: "Forwarded", 2: "Forwarded Stub",
	3: "Index", 4: "Blob", 5: "Ghost Index",
	6: "Ghost Data"}

type DataRow struct {
	// min len 9 bytes
	StatusA               uint8  //1
	StatusB               uint8  //1
	NofColsOffset         uint16 //2
	FixedLenCols          []byte //1-
	NumberOfCols          uint16 //2
	NullBitmap            uint16 //1-2
	NumberOfVarLengthCols uint16 //0-
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

func (colinfo SysColpars) ShowData() {

	fmt.Printf("col id %d offset %d len %d \n",
		colinfo.ObjectId, colinfo.Colorder, colinfo.Colsize)

}
