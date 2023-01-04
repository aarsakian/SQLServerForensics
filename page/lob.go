package page

import "MSSQLParser/utils"

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
	utils.Unmarshal(data[:8], root)
	for curLink := uint16(0); curLink < root.CurLinks; curLink++ {
		var internalBodyPointer *LOBRootBody = new(LOBRootBody)
		utils.Unmarshal(data[8+16*curLink:8+16*(curLink+1)], internalBodyPointer)
		internalPointers = append(internalPointers, *internalBodyPointer)
	}
	root.InternalPointers = internalPointers
	lob.Root = root
}

func (lob *LOB) ParseInternal(data []byte) {
	var internal *LOBInternal = new(LOBInternal)
	var dataPointers []LOBInternalBody
	for curLink := uint16(0); curLink < internal.CurLinks; curLink++ {
		var dataPointer *LOBInternalBody = new(LOBInternalBody)
		utils.Unmarshal(data[8+16*curLink:8+16*(curLink+1)], dataPointer)
		dataPointers = append(dataPointers, *dataPointer)
	}
	internal.DataPointers = dataPointers
	lob.Internal = internal
}

func (lob LOB) walk(lobPages PageMap) []byte {

	return []byte{}

}

func (lob LOB) GetData(lobPages PageMap) []byte {
	if lob.Type == 3 {
		return lob.Data
	} else if lob.Type == 5 {
		return lob.walk(lobPages)
	}
	return []byte{}
}
