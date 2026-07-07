package page

import (
	datac "MSSQLParser/data"
	"MSSQLParser/logger"

	"MSSQLParser/utils"
	"fmt"
	"math"
)

// If a clustered index is created, must be unique so that nonclustered index entries can point to exactly one specific row.
// if the index isn't defined as unique, the duplicated key values include a uniquifier  a 4-byte integer to make each nonunique key value unique.
// , all nonclustered index entries must refer to exactly one row. Because that pointer is the clustering key in SQL Server,
//the clustering key must be unique
// clustered index root -> intermediate level rowid( child fileId+child PageId) +key value and so on
// The leaf level of a clustered index is the data itself, the data is copied and ordered based on the clustering key r
// Each row in the non-leaf levels has one entry for every page of the level below,  this entry includes an index key value and a 6-byte pointer to reference the page

// a table can have only one clustered index.
// intermediate level one row for each leaf size =key value (e.g. int = 4 byte) + rid (8 bytes) + 1 overhead (fixed)
//
// non clustered key value (e.g. int = 4 byte) + rid (8 bytes) + 1 overhead (fixed)
// root->childpageID leaf level have value of 0
//row structure of a clustered index is no different from the row structure of a heap,
// except in one case: when the clustering key isnβ€™t defined with the UNIQUE attribute.
//In this case, SQL Server must guarantee uniqueness internally, and to do this, each duplicate row requires an additional uniquifier value.

type IndexRows []IndexRow

type IndexNoNLeafClustered struct {
	ChildPageID uint32
	FileID      uint16
	ChildFileID uint16 //? not sure
	RowSize     uint16
	KeyValue    []byte
}

type IndexNoNLeaf struct {
	KeyValue    []byte //0-?
	ChildPageID uint32
	ChildFileID uint16
}

type IndexLeafClustered struct {
	KeyValue []byte
}

type IndexLeafNoNClustered struct {
	KeyValue    []byte //0-?
	ChildPageID uint32
	ChildFileID uint16
	RowId       uint16
}

type IndexRow struct {
	// index record on a non-clustered index at leaf level,,
	//Only if the index row has nullable columns are the field called NumberofCols and the null bitmap both present
	StatusA          uint8 //0-1
	NoNLeaf          *IndexNoNLeaf
	NoNLeafClustered *IndexNoNLeafClustered
	LeafNoNClustered *IndexLeafNoNClustered
	LeafClustered    *IndexLeafClustered

	FixedLenCols          []byte //0-
	NumberOfCols          uint16 //
	NullBitmap            []byte //
	NumberOfVarLengthCols uint16 //0-
	VarLengthColOffsets   []int16
	VarLenCols            *datac.DataCols
}

func (indexRow *IndexRow) Parse(data []byte, offset int, fixedColsLen uint16) {

	indexRow.StatusA = data[0]
	if !indexRow.IsIndexRecord() {
		logger.Mslogger.Warning(fmt.Sprintf("Not an index record at offset %d", offset))
		return
	}

	indexRow.FixedLenCols = append([]byte(nil), data[1:fixedColsLen]...)
	currOffset := fixedColsLen
	if indexRow.HasNullBitmap() {
		indexRow.NumberOfCols = utils.ToUint16(data[currOffset : currOffset+2])
		bytesNeeded := uint16(math.Floor(float64(indexRow.NumberOfCols) / 8))
		if bytesNeeded == 0 {
			bytesNeeded = 1
		} else if int(bytesNeeded) > len(data)-int(currOffset)-2 {
			logger.Mslogger.Warning(fmt.Sprintf("bytes needd exceeds buffer %d buf %d curoffset %d ",
				bytesNeeded, len(data), currOffset))
			return
		}

		indexRow.NullBitmap = data[currOffset+2 : currOffset+2+bytesNeeded]
		currOffset += 2 + bytesNeeded
	}
	if indexRow.HasVarLenCols() && int(currOffset) < len(data) {

		indexRow.ProcessVaryingCols(data, int(currOffset))
	}

}

func (indexRow *IndexRow) ProcessVaryingCols(data []byte, offset int) {

	indexRow.NumberOfVarLengthCols = utils.ToUint16(data[offset : offset+2])

	baseOffset := offset + 2 + 2*int(indexRow.NumberOfVarLengthCols) // move offset to the start of varying len cols
	if baseOffset > len(data) {
		logger.Mslogger.Warning(fmt.Sprintf("base offset %d exceeds buffer length %d", baseOffset, len(data)))
		return
	}
	for i := 0; i < int(indexRow.NumberOfVarLengthCols); i++ {
		if 4+2*i > len(data) {
			logger.Mslogger.Warning(fmt.Sprintf("var len %d col exceeds buffer by %d", i, 4+2*i-len(data)))
			break
		}
		varLenColOffset := utils.ToInt16(data[offset+2+2*i : offset+4+2*i])
		indexRow.VarLengthColOffsets = append(indexRow.VarLengthColOffsets, varLenColOffset)
	}

	var datacols datac.DataCols

	for idx, varLenColOffset := range indexRow.VarLengthColOffsets {

		//varlencoloffset is the offset to the end of the varying length column,
		// so we need to calculate the length of the column by subtracting the base offset from the varlencoloffset
		if varLenColOffset <= int16(baseOffset) {
			logger.Mslogger.Warning("var len column offset is after the base offset")
			break
		} else if varLenColOffset > int16(len(data)) {
			logger.Mslogger.Warning(fmt.Sprintf("var len column offset %d exceeds buffer length %d", varLenColOffset, len(data)))
			break
		} else if varLenColOffset <= 0 {
			logger.Mslogger.Warning(fmt.Sprintf("var len column offset %d is less than or equal to 0", varLenColOffset))
			break
		}
		dst := make([]byte, int(varLenColOffset)-baseOffset) //buffer for varying le cols
		copy(dst, data[baseOffset:int(varLenColOffset)])
		datacols = append(datacols,
			datac.DataCol{Id: idx, Content: dst, Offset: uint16(baseOffset + offset)})
		baseOffset = int(varLenColOffset) // move offset to the end of the current varying len col
	}
	indexRow.VarLenCols = &datacols
}

func (indexRow IndexRow) IsIndexRecord() bool {
	return indexRow.StatusA&6 == 6
}

func (indexRow IndexRow) HasNullBitmap() bool {
	return indexRow.StatusA&16 == 16
}

func (indexRow IndexRow) HasVarLenCols() bool {
	return indexRow.StatusA&32 == 32
}
func (indexRow IndexRow) IsNonLeafRecord() bool {
	return indexRow.StatusA&38 == 38
}

func (indexRow IndexRow) IsLeafNoNClusteredRecord() bool {
	return indexRow.StatusA&22 == 22
}

func (indexRow IndexRow) IsGhostRecord() bool {
	return indexRow.StatusA&10 == 10
}

func (indexRow IndexRow) ShowData() {
	if indexRow.NoNLeafClustered != nil {
		fmt.Printf("Non Leaf CLustered Child FileID %d PageID %d  Key %x RowSize %d\n",
			indexRow.NoNLeafClustered.ChildFileID,
			indexRow.NoNLeafClustered.ChildPageID, indexRow.NoNLeafClustered.KeyValue,
			indexRow.NoNLeafClustered.RowSize)
	} else if indexRow.LeafNoNClustered != nil {
		fmt.Printf("Leaf Non Clustered ChildID %d Key %x RowID %d\n",
			indexRow.LeafNoNClustered.ChildPageID,
			indexRow.LeafNoNClustered.KeyValue, indexRow.LeafNoNClustered.RowId)
	} else if indexRow.NoNLeaf != nil {
		fmt.Printf("Non Leaf %d key %x\n", indexRow.NoNLeaf.ChildPageID,
			indexRow.NoNLeaf.KeyValue)
	}

}
