package page

import (
	"MSSQLParser/utils"
	"fmt"
	"unsafe"
)

//clustered index root -> intermediate level rowid( child fileId+child PageId) +key value and so on
//intermediate level one row for each leaf size =key value (e.g. int = 4 byte) + rid (8 bytes) + 1 overhead (fixed)

// non clustered key value (e.g. int = 4 byte) + rid (8 bytes) + 1 overhead (fixed)
// root->childpageID leaf level have value of 0
type IndexRows []IndexRow

type IndexIntermediateClustered struct {
	PageID      uint32
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

type IndexLeafNoNClustered struct {
	KeyValue    []byte //0-?
	ChildPageID uint32
	ChildFileID uint16
	RowId       uint16
}

type IndexRow struct {
	// index record on a non-clustered index at leaf level,,
	//Only if the index row has nullable columns are the field called NumberofCols and the null bitmap both present
	StatusA               uint8 //0-1
	NoNLeaf               *IndexNoNLeaf
	IntermediateClustered *IndexIntermediateClustered
	LeafNoNClustered      *IndexLeafNoNClustered

	FixedLenCols          []byte //0-
	NumberOfCols          uint16 //
	NullBitmap            []byte //
	NumberOfVarLengthCols uint16 //0-
	VarLengthColOffsets   []int16
	VarLenCols            *DataCols
}

func (indexRow *IndexRow) Parse(data []byte) {
	indexRow.StatusA = data[0]
	if indexRow.IsIntermediateClusteredRecord() {
		indexIntermediate := new(IndexIntermediateClustered)
		utils.Unmarshal(data[1:], indexIntermediate)

		structSize := int(unsafe.Sizeof(indexIntermediate))
		indexIntermediate.KeyValue = make([]byte, len(data[1+structSize:]))
		copy(indexIntermediate.KeyValue, data[1+structSize:])

		indexRow.IntermediateClustered = indexIntermediate

	} else if indexRow.IsRootRecordClustered() && len(data) > 6 { // root ?

		indexNoNLeaf := new(IndexNoNLeaf)

		utils.Unmarshal(data[len(data)-6:], indexNoNLeaf)

		structSize := int(unsafe.Sizeof(indexNoNLeaf))
		if len(data) > structSize {
			indexNoNLeaf.KeyValue = make([]byte, len(data[1:len(data)-structSize]))
			copy(indexNoNLeaf.KeyValue, data[1:len(data)-structSize])
			indexRow.NoNLeaf = indexNoNLeaf
		}

	} else if indexRow.IsLeafNoNClusteredRecord() {
		indexLeafNoNClustered := new(IndexLeafNoNClustered)
		utils.Unmarshal(data[len(data)-8:], indexLeafNoNClustered)

		structSize := int(unsafe.Sizeof(indexLeafNoNClustered))
		indexLeafNoNClustered.KeyValue = make([]byte, len(data[1:len(data)-structSize]))
		copy(indexLeafNoNClustered.KeyValue, data[1:len(data)-structSize])

		indexRow.LeafNoNClustered = indexLeafNoNClustered
	}

}

func (indexRow *IndexRow) ProcessVaryingCols(data []byte, offset int) {
	baseOffset := int16(1 + len(indexRow.FixedLenCols) + 2 + len(indexRow.NullBitmap) + 2 + len(indexRow.VarLengthColOffsets)*2) //status + len of fixed cols
	var datacols DataCols

	for idx, varLenColOffset := range indexRow.VarLengthColOffsets {

		dst := make([]byte, varLenColOffset-baseOffset) //buffer for varying le cols
		copy(dst, data[baseOffset:varLenColOffset])
		datacols = append(datacols,
			DataCol{id: idx, Content: dst, offset: uint16(varLenColOffset)})
		baseOffset = varLenColOffset
	}
	indexRow.VarLenCols = &datacols
}

func (indexRow IndexRow) IsPrimary() bool {
	return indexRow.StatusA == 0
}

func (indexRow IndexRow) IsIndexRecord() bool {
	return indexRow.StatusA&3 == 3
}

func (indexRow IndexRow) IsIntermediateClusteredRecord() bool {
	return indexRow.StatusA&38 == 38
}

func (indexRow IndexRow) IsRootRecordClustered() bool {
	return indexRow.StatusA&6 == 6
}

func (indexRow IndexRow) IsLeafNoNClusteredRecord() bool {
	return indexRow.StatusA&22 == 22
}

func (indexRow IndexRow) IsGhostRecord() bool {
	return indexRow.StatusA&5 == 5
}

func (indexRow IndexRow) ShowData() {
	if indexRow.IntermediateClustered != nil {
		fmt.Printf("Intermediate CLustered Child FileID %d PageID %d  Key %x RowSize %d\n",
			indexRow.IntermediateClustered.ChildFileID,
			indexRow.IntermediateClustered.PageID, indexRow.IntermediateClustered.KeyValue,
			indexRow.IntermediateClustered.RowSize)
	} else if indexRow.LeafNoNClustered != nil {
		fmt.Printf("Leaf Non Clustered ChildID %d Key %x RowID %d\n",
			indexRow.LeafNoNClustered.ChildPageID,
			indexRow.LeafNoNClustered.KeyValue, indexRow.LeafNoNClustered.RowId)
	} else if indexRow.NoNLeaf != nil {
		fmt.Printf("Non Leaf %d key %x\n", indexRow.NoNLeaf.ChildPageID,
			indexRow.NoNLeaf.KeyValue)
	}

}
