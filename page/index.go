package page

type IndexRow struct {
	// min size is 6 bytes for an index record on a non-clustered index at leaf level,,
	StatusA               uint8
	NumberOfCols          uint16 //2
	NullBitmap            uint16 //1-2
	NumberOfVarLengthCols uint16 //0-
	DataCols              *DataCols
}

func (indexRow IndexRow) ShowData() {

}
