package page

import "fmt"

//clustered index root -> intermediate level rowid( child fileId+child PageId) +key value and so on
//intermediate level one row for each leaf size =key value (e.g. int = 4 byte) + rid (8 bytes) + 1 overhead (fixed)

//non clustered key value (e.g. int = 4 byte) + rid (8 bytes) + 1 overhead (fixed)
//root->childpageID leaf level have value of 0
type IndexRows []IndexRow

type IndexRow struct {
	// index record on a non-clustered index at leaf level,,
	//Only if the index row has nullable columns are the field called NumberofCols and the null bitmap both present
	StatusA               uint8  //1-2
	FixedLenCols          []byte //0-
	NumberOfCols          uint16 //
	NullBitmap            []byte //
	NumberOfVarLengthCols uint16 //0-
	VarLengthColOffsets   []int16
	VarLenCols            *DataCols
}

func (indexRow *IndexRow) ProcessVaryingCols(data []byte, offset int) {
	baseOffset := int16(1 + len(indexRow.FixedLenCols) + 2 + len(indexRow.NullBitmap) + 2 + len(indexRow.VarLengthColOffsets)*2) //status + len of fixed cols
	var datacols DataCols

	for idx, varLenColOffset := range indexRow.VarLengthColOffsets {

		dst := make([]byte, varLenColOffset-baseOffset) //buffer for varying le cols
		copy(dst, data[baseOffset:varLenColOffset])
		datacols = append(datacols,
			DataCol{id: idx, content: dst, offset: uint16(varLenColOffset)})
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

func (indexRow IndexRow) IsGhostRecord() bool {
	return indexRow.StatusA&5 == 5
}

func (indexRow IndexRow) ShowData() {
	fmt.Printf("Fixed Cols %x ", indexRow.FixedLenCols)
	for _, datacol := range *indexRow.VarLenCols {
		fmt.Printf(" Varying cols %x ", datacol.content)

	}
	fmt.Printf("\n")
}
