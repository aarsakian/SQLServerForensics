package data

import "MSSQLParser/utils"

type TextBlob struct {
	Timestamp uint32 //internal version stamp for concurrency
	TextId    uint32 //Lob chain identifier
	RowId     utils.RowId
}

type InlineBLob24 struct {
	Type       uint8
	IndexLevel uint16
	Unused     uint8
	UpdateSeq  uint16
	Timestamp  uint32
	Link       uint16
	Length     uint32
	RowId      utils.RowId //12-
	Inlines    []InlineBLob16
}

type InlineBLob16 struct { //points to text lob
	Length uint32
	RowId  utils.RowId //4-
}

func (inlineBlob24 *InlineBLob24) Process(data []byte) {
	var rowId *utils.RowId = new(utils.RowId)
	utils.Unmarshal(data, inlineBlob24)
	utils.Unmarshal(data[16:], rowId)
	inlineBlob24.RowId = *rowId

	inlineOffset := 24
	for inlineOffset < len(data) {
		inlineBlob16 := new(InlineBLob16)
		utils.Unmarshal(data[inlineOffset:], inlineBlob16)
		utils.Unmarshal(data[inlineOffset+4:], rowId)
		inlineBlob16.RowId = *rowId
		inlineOffset += 12
		inlineBlob24.Inlines = append(inlineBlob24.Inlines, *inlineBlob16)
	}
}

func (textBlob *TextBlob) Process(data []byte) {
	utils.Unmarshal(data, textBlob)
}
