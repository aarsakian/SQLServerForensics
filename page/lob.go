package page

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
)

type LOBS []LOB

//RowId (DATA page) -> Type 5 Text Page -> Type 2 Text Page =>Type 3 Text Page (leaves)

// large objects storage
type LOB struct { //generic structure
	StatusA  uint8
	StatusB  uint8
	Length   uint16
	Id       uint64
	Type     uint16
	Root     *LOBRoot
	Internal *LOBInternal
	Data     []byte //14-
}

type LOBRoot struct { // Text lob type 5
	MaxLinks         uint16
	CurLinks         uint16
	Level            uint16
	Unused           [4]byte
	InternalPointers []LOBRootBody
}

type LOBRootBody struct {
	Length     uint32
	PageId     uint32
	FileId     uint16
	SlotNumber uint16
}

type LOBInternal struct { //text lob type 2
	MaxLinks     uint16
	CurLinks     uint16
	Level        uint16
	DataPointers []LOBInternalBody
}

type Body interface {
}

type LOBInternalBody struct {
	Length     uint32
	Unused     [4]byte
	PageId     uint32
	FileId     uint16
	SlotNumber uint16
}

func (lob *LOB) ParseRoot(data []byte) {
	var root *LOBRoot = new(LOBRoot)
	var internalPointers []LOBRootBody
	utils.Unmarshal(data[:10], root)
	for curLink := uint16(0); curLink < root.CurLinks; curLink++ {
		var internalBodyPointer *LOBRootBody = new(LOBRootBody)
		utils.Unmarshal(data[10+12*curLink:10+12*(curLink+1)], internalBodyPointer)
		internalPointers = append(internalPointers, *internalBodyPointer)
	}
	root.InternalPointers = internalPointers
	lob.Root = root
}

func (lob *LOB) ParseInternal(data []byte) {
	var internal *LOBInternal = new(LOBInternal)
	var dataPointers []LOBInternalBody
	utils.Unmarshal(data[:6], internal)
	for curLink := uint16(0); curLink < internal.CurLinks; curLink++ {
		var dataPointer *LOBInternalBody = new(LOBInternalBody)
		utils.Unmarshal(data[6+16*curLink:6+16*(curLink+1)], dataPointer)
		dataPointers = append(dataPointers, *dataPointer)
	}
	internal.DataPointers = dataPointers
	lob.Internal = internal
}

func (lob LOB) walk(lobPages PagesPerId[uint32], textLobPages PagesPerId[uint32],
	dataParts [][]byte, textTimestamp uint, parentPageId uint32) [][]byte {

	if lob.Type == 2 {
		for _, dataLob := range lob.Internal.DataPointers { //points to lob type 3
			lobPage := lobPages.GetFirstPage(dataLob.PageId)
			for _, lob := range lobPage.LOBS {
				if lob.Id != uint64(textTimestamp) {
					continue
				}
				dataParts = append(dataParts, lob.Data)
			}
		}

	} else if lob.Type == 3 && lob.Id == uint64(textTimestamp) {
		dataParts = append(dataParts, lob.Data)
	} else if lob.Type == 5 { // type 5
		for _, internalLob := range lob.Root.InternalPointers { //internal lob type 2 or type 3

			if parentPageId == internalLob.PageId { //cyclic reference protection
				continue
			}

			var lobPage Page

			internalLobPages := lobPages.GetPages(internalLob.PageId)
			if len(internalLobPages) == 0 {
				msg := "Lob does not have pages."
				mslogger.Mslogger.Warning(msg)
				continue
			} else {
				lobPage = internalLobPages[0]
			}

			if lobPage.Header.PageId == 0 { // lob Pages does not contains thiss page id
				lobPage = textLobPages.GetFirstPage(internalLob.PageId)
			}

			for _, lob := range lobPage.LOBS {

				if lob.Id != uint64(textTimestamp) {
					continue
				}

				dataParts = lob.walk(lobPages, textLobPages, dataParts, textTimestamp, internalLob.PageId)

			}

		}
	}
	return dataParts

}
