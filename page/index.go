package page

import (
	"MSSQLParser/utils"
	"fmt"
	"unsafe"
)

// If a clustered index is created, must be unique so that nonclustered index entries can point to exactly one specific row.
// if the index isn't defined as unique, the duplicated key values include a uniquifier  a 4-byte integer to make each nonunique key value unique.
// , all nonclustered index entries must refer to exactly one row. Because that pointer is the clustering key in SQL Server, the clustering key must be unique
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

	} else if indexRow.IsLeafNoNClusteredRecord() && len(data) == 6 {
		indexNoNLeaf := new(IndexNoNLeaf)

		utils.Unmarshal(data[len(data)-6:], indexNoNLeaf)
		indexRow.NoNLeaf = indexNoNLeaf

	} else if indexRow.IsLeafNoNClusteredRecord() && len(data) > 8 {
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
