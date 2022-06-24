package page

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"
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

// syscolpars a contains a row for every column in a table
type SysColpars struct {
	unknown  [4]byte
	Id       uint32 //4 -8
	Number   uint16 //8-10
	ColId    uint16 //10 -12
	Colorder uint16 //12 - 14
	Xtype    uint8  // 14 sys.sysscalartypes.xtype.
	Utype    uint32 //15-19 sys.sysscalartypes.id
	Colsize  uint16 //19-21
	unknown4 [30]byte
	Length   uint16 // 51-53  -1 = contains varlen types
	Name     string
}

//stores all table information used in the database
type Sysschobjs struct {
	Id       int32
	Nsid     uint32
	Nsclass  uint8
	Status   uint32
	Type     string //2 bytes
	Name     string
	created  [8]byte
	modified [8]byte
}

type DataRows []interface {
	ShowData()
}

type DataCols []DataCol

func (syscolpars SysColpars) GetType() string {
	if syscolpars.Xtype == 0x38 {
		return "Static"
	}
	return ""
}

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
	VarLengthColOffsets   []uint16
	DataCols              *DataCols
}

func (dataRow DataRow) GetFlags() string {
	recordType := DataRecord[dataRow.StatusA&6]
	nullBitmap := (map[bool]string{true: "NULL Bitmap"})[dataRow.StatusA&8 == 8]
	varLenCols := (map[bool]string{true: "Var length Cols"})[dataRow.StatusA&10 == 10]
	return strings.Join([]string{recordType, nullBitmap, varLenCols}, " ")
}

func (dataRow DataRow) GetVarCalOffset() uint16 {

	return dataRow.NofColsOffset + uint16(unsafe.Sizeof(dataRow.NumberOfCols)) + uint16(unsafe.Sizeof(dataRow.NullBitmap)) +
		uint16(unsafe.Sizeof(dataRow.NumberOfVarLengthCols)) +
		uint16(reflect.ValueOf(dataRow.VarLengthColOffsets).Len()*2)
}

func (dataRow DataRow) ShowData() {
	for _, dataCol := range *dataRow.DataCols {
		fmt.Printf("col id %d offset %x len %d \n",
			dataCol.id, dataCol.offset, reflect.ValueOf(dataCol.content).Len())
	}
}

func (colinfo SysColpars) ShowData() {

	fmt.Printf("col id %d offset %d len %d %s \n",
		colinfo.Id, colinfo.Colorder, colinfo.Colsize, colinfo.Name)

}
