package page

type IndexRow struct {
	// min size is 6 bytes for an index record on a non-clustered index at leaf level,,
	StatusA               uint8   //1-2
	FixedLenColsOffset    *uint16 //2-4  pminlen-1, does not exist infer by page header
	FixedLenCols          []byte  //fixed portion of index
	NumberOfCols          uint16  //4-6
	NullBitmap            []byte  //6-7
	NumberOfVarLengthCols uint16  //0-
	VarLengthColOffsets   []int16
	VarLenCols            *DataCols
}

func (indexRow IndexRow) ShowData() {

}
