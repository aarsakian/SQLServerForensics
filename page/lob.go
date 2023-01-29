package page

import (
	"MSSQLParser/utils"
	"bytes"
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

func (lob LOB) walk(lobPages PageMapIds, textLobPages PageMapIds, dataParts [][]byte) [][]byte {

	if lob.Type == 2 {
		for _, dataLob := range lob.Internal.DataPointers {
			lobPage := lobPages[dataLob.PageId]
			for _, lob := range lobPage.LOBS {
				dataParts = append(dataParts, lob.Data)
			}
		}

	} else {
		for _, internalLob := range lob.Root.InternalPointers {
			lobPage := textLobPages[internalLob.PageId]
			for _, lob := range lobPage.LOBS {
				dataParts = lob.walk(lobPages, textLobPages, dataParts)
			}
		}
	}
	return dataParts

}

func (lobs LOBS) GetData(lobPages PageMapIds, textLobPages PageMapIds) []byte {

	var dataParts [][]byte
	for _, lob := range lobs {
		if lob.Type == 3 && len(lobs) == 1 { //dataRow has only one lob and it is leaf
			dataParts = append(dataParts, lob.Data)
		} else if lob.Type == 5 {
			dataParts = lob.walk(lobPages, textLobPages, dataParts)
		}
	}

	return bytes.Join(dataParts, []byte{})
}
