package page

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

// SQL Server Record Header - StatusBitsA flags
// bit 1->3 from right to left 0000 0000
// 0000 0001 = Special versioning 1
// 0000 0010 = Forwarding 2
// 0000 0110 = Index record 6
// 0000 1000 = Blob fragment 8
// 0000 1010 = Ghost index record A
// 0000 1100 = Ghost data record D
// 0000 1110 = Ghost version record E
// 0001 0000 = Null bitmap exists 10
// 0010 0000 = Has variable length columns 20
// 0100 0000 = Versioning 40
// 1000 0000 = Internal/system record
const (
	BitVersioning = 0x40 // Row has versioning info (RCSI/SI)
	BitForwarded  = 0x02 // Row has been forwarded
	BitForwarding = 0x04 // Row has been forwarded
	BitIndex      = 0x06 // Row is an index record
	BitHasLOB     = 0x08 // Row has LOB or Row-Overflow columns

	BitGhostIndex   = 0x0A // Ghosted (logically deleted)
	BitGhostData    = 0x0D // Ghosted (logically deleted)
	BitGhostVersion = 0x0E // Ghosted (logically deleted)
	BitNullBitmap   = 0x10 // Has NULL bitmap
	BitVarLenCols   = 0x20 // Has variable-length columns

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
	Content      []byte
	InlineBlob24 *InlineBLob24
	InlineBlob16 *InlineBLob16
}

type DataRows []DataRow

type RowIds []utils.RowId

type DataCols []DataCol //holds varying len cols

type InlineBLob24 struct {
	Type       uint8
	IndexLevel uint16
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

type TagVersion struct {
	RowId utils.RowId
	XSN   [6]byte
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
	VersioningInfo        *TagVersion
	VarLenCols            *DataCols
	Carved                bool
	SystemTable           SystemTable
}

func GetRowType(statusA byte) string {

	if statusA&BitGhostVersion == BitGhostVersion {
		return "Ghost Version Record"
	} else if statusA&BitGhostData == BitGhostData {
		return "Ghost Data Record"
	} else if statusA&BitGhostIndex == BitGhostIndex {
		return "Ghost Index Record"
	} else if statusA&BitForwarding == BitForwarding {
		return "Forwarding Stub Record"
	} else if statusA&BitForwarded == BitForwarded {
		return "Forwarded Record"
	} else {
		return "Primary Record"
	}
}

func (dataRow DataRow) GetFlags() string {
	var flags strings.Builder
	if dataRow.StatusA&BitVersioning == BitVersioning {
		flags.WriteString("Versioning ")
	}
	if dataRow.StatusA&BitGhostIndex == BitGhostIndex {
		flags.WriteString("Ghost Index ")
	}
	if dataRow.StatusA&BitGhostData == BitGhostData {
		flags.WriteString("Ghost Data ")
	}
	if dataRow.StatusA&BitGhostVersion == BitGhostVersion {
		flags.WriteString("Ghost Version ")
	}
	if dataRow.StatusA&BitNullBitmap == BitNullBitmap {
		flags.WriteString("Has Null Bitmap ")
	}
	if dataRow.StatusA&BitVarLenCols == BitVarLenCols {
		flags.WriteString("Has Var Length Columns ")
	}
	if dataRow.StatusA&BitForwarded == BitForwarded {
		flags.WriteString("Forwarded Row ")
	}
	if dataRow.StatusA&BitForwarding == BitForwarding {
		flags.WriteString("Forwarding Stub ")
	}
	if dataRow.StatusA&BitHasLOB == BitHasLOB {
		flags.WriteString("Has LOB/Row-Overflow Columns ")
	}

	return flags.String()
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

func (dataCol DataCol) GetLOBPage() (utils.RowId, uint32) {
	if dataCol.hasBlob16() {
		return dataCol.InlineBlob16.RowId, dataCol.InlineBlob16.Timestamp // needs check for more rowids
	} else if dataCol.hasBlob24() {
		return dataCol.InlineBlob24.RowId, dataCol.InlineBlob24.Timestamp // needs check for more rowids
	}
	return utils.RowId{}, 0
}

func (dataRow DataRow) GetBloBInfo(colNum uint16) (utils.RowId, uint32) {
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
	msg := fmt.Sprintf("Fixed len cols end at %d  : %x",
		4+reflect.ValueOf(dataRow.FixedLenCols).Len(), dataRow.FixedLenCols)
	mslogger.Mslogger.Info(msg)
	fmt.Printf(msg + "\nVar len info cols: \n")
	if dataRow.VarLenCols == nil {
		msg := "No Var Len cols found"
		fmt.Printf(msg + "\n")
		mslogger.Mslogger.Warning(msg)
		return
	}
	for _, dataCol := range *dataRow.VarLenCols {
		fmt.Printf("starts at %d len  %d : %x ",
			dataCol.offset, reflect.ValueOf(dataCol.Content).Len(), dataCol.Content)
	}
	fmt.Printf("\n")
}

func (dataRow *DataRow) ProcessVaryingCols(data []byte, offset int) int { // data per slot
	var datacols DataCols
	var inlineBlob24 *InlineBLob24
	var inlineBlob16 *InlineBLob16
	startVarColOffset := dataRow.GetVarCalOffset()
	for idx, endVarColOffset := range dataRow.VarLengthColOffsets {
		msg := fmt.Sprintf("%d var col at %d", idx, offset+int(startVarColOffset))
		mslogger.Mslogger.Info(msg)

		// -1 NULL, -2 Row Overflow, -3 Sparse Null, -4 Sparse Non Null
		if endVarColOffset < -4 {
			msg := fmt.Sprintf("invalid %d negative offset %d var col at %d", idx, endVarColOffset, offset+int(startVarColOffset))
			mslogger.Mslogger.Warning(msg)
			break

		}

		if len(dataRow.VarLengthColOffsets)*2 >= 8192 {
			msg := fmt.Sprintf("number of val len col offsets %d exceeds page size", len(dataRow.VarLengthColOffsets))
			mslogger.Mslogger.Warning(msg)

			break
		}

		if endVarColOffset <= startVarColOffset {
			continue
		} else if int(startVarColOffset) > len(data) {
			break
		} else if int(endVarColOffset) > len(data) || endVarColOffset < 0 {
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

		if inlineBlob16 != nil {
			datacols = append(datacols,
				DataCol{id: idx, Content: cpy, offset: uint16(startVarColOffset), InlineBlob16: inlineBlob16})
			inlineBlob16 = nil
		} else if inlineBlob24 != nil {
			datacols = append(datacols,
				DataCol{id: idx, Content: cpy, offset: uint16(startVarColOffset), InlineBlob24: inlineBlob24})
			inlineBlob24 = nil
		} else {
			datacols = append(datacols,
				DataCol{id: idx, Content: cpy, offset: uint16(startVarColOffset)})
		}

	}
	dataRow.VarLenCols = &datacols

	if dataRow.NumberOfVarLengthCols > 0 && int(dataRow.NumberOfVarLengthCols) != len(dataRow.VarLengthColOffsets) { // last varlencol
		mslogger.Mslogger.Warning("Mismatch in var len col parsing real differs with declared number of cols.")
		return int(dataRow.VarLengthColOffsets[int(dataRow.NumberOfVarLengthCols)-len(dataRow.VarLengthColOffsets)])
	} else {
		return int(dataRow.VarLengthColOffsets[dataRow.NumberOfVarLengthCols-1])
	}

}

func (dataRow DataRow) PrintNullBitmapToBit(nofCols int) string {
	var bitrepresentation string
	for valpos, val := range dataRow.NullBitmap {
		bitval := utils.AddMissingBits(strconv.FormatUint(uint64(val), 2), nofCols, valpos)
		bitrepresentation = strings.Join([]string{bitval, bitrepresentation}, "")
	}
	return bitrepresentation
}

func (dataRow DataRow) ProcessData(colId uint16, colsize int16, startoffset int16,
	static bool, valorder uint16, lobPages PagesPerId[uint32], textLobPages PagesPerId[uint32]) ([]byte, error) {

	if static {
		fixedLenColsOffset := 4 // include status flag nofcols
		if int(colsize) > len(dataRow.FixedLenCols) {
			msg := fmt.Sprintf("Col Id %d Column size %d exceeded fixed len cols size %d",
				colId, colsize, len(dataRow.FixedLenCols))

			mslogger.Mslogger.Error(msg)
			return nil, errors.New(msg)
		} else if int(startoffset) > len(dataRow.FixedLenCols)+fixedLenColsOffset {
			msg := fmt.Sprintf("Col Id %d column start offset %d exceeded available area of fixed len cols by %d",
				colId, startoffset, int(startoffset)-len(dataRow.FixedLenCols))

			mslogger.Mslogger.Error(msg)
			return nil, errors.New(msg)
		} else if int(startoffset)+int(colsize) > len(dataRow.FixedLenCols)+fixedLenColsOffset {
			msg := fmt.Sprintf("Col Id %d End offset %d exceeded available area of fixed len cols by %d ?",
				colId, int(startoffset)+int(colsize), int(startoffset)+int(colsize)-len(dataRow.FixedLenCols))

			mslogger.Mslogger.Error(msg)
			return nil, errors.New(msg)
		} else if startoffset < 4 {
			msg := fmt.Sprintf("Col id %d start offset %d is less than 4 fixed len cols offset",
				colId, startoffset)

			mslogger.Mslogger.Error(msg)
			return nil, errors.New(msg)
		} else {
			return dataRow.FixedLenCols[startoffset-int16(fixedLenColsOffset) : startoffset+colsize-int16(fixedLenColsOffset)], nil //offset is from start of datarow
		}

	} else {
		if dataRow.VarLenCols == nil {
			msg := fmt.Sprintf("No var len cols found at offset %d", startoffset)
			mslogger.Mslogger.Error(msg)
			return nil, errors.New(msg)
		} else if len(*dataRow.VarLenCols) <= int(valorder) {
			// should had bitmap set to 1 however it is not expiremental
			msg := fmt.Sprintf("Number of var len cols is less than the asked col %d col offset within datarow %d",
				colId, startoffset)
			mslogger.Mslogger.Error(msg)
			return nil, errors.New(msg)

		}

		rowId, textTimestamp := dataRow.GetBloBInfo(valorder)
		if !lobPages.IsEmpty() && rowId.FileId != 0 { //only when there are lobpages proceed

			lobPage := lobPages.GetFirstPage(rowId.PageId)

			return lobPage.GetLobData(lobPages, textLobPages, uint(rowId.SlotNumber), uint(textTimestamp)), nil // might change
		} else {
			return (*dataRow.VarLenCols)[valorder].Content, nil
		}

	}

}

func (dataRow *DataRow) Parse(data []byte, offset int, pageType int32) int {

	dataRowSize, _ := utils.Unmarshal(data, dataRow)
	if len(data) > 14 && dataRow.HasVersionTag() {
		dataRow.VersioningInfo = new(TagVersion)
		utils.Unmarshal(data[len(data)-14:], dataRow.VersioningInfo)
	}
	if dataRow.HasVarLenCols() && len(dataRow.VarLengthColOffsets) != 0 {
		return dataRow.ProcessVaryingCols(data, offset)

	} else {
		mslogger.Mslogger.Info("No var len col offsets found")
		return dataRowSize
	}

}

func (dataRow *DataRow) Process(systemtable SystemTable) {

	utils.Unmarshal(dataRow.FixedLenCols, systemtable)
	dataRow.SystemTable = systemtable

}
