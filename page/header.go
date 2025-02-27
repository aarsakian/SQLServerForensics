package page

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
)

type Header struct {
	Version        uint8     //1
	Type           uint8     // 1-2
	TypeFlagBits   uint8     //2-3
	Level          uint8     // 0 = leaf
	FlagBits       [2]byte   //4-6
	IndexId        uint16    //6-8  0 = Heap 1 = Clustered Index  (AllocUnitId.idInd))
	PrevPage       uint32    //8-12
	PreviousFileId uint16    //12-14
	PMinLen        uint16    //14-16  size of fixed len records
	NextPage       uint32    //16-20
	NextPageFileId uint16    //20-22
	SlotCnt        uint16    //22-24   number of slots (records) that hold data
	ObjectId       int32     //24-28 AllocUnitId.idObj
	FreeCnt        uint16    //28-30 free space in bytes
	FreeData       uint16    //30-32 offset from the start of the page to the first byte after the last record
	PageId         uint32    //32-36
	FragId         uint32    //36-40
	LSN            utils.LSN //40-50  LSN of the last log record that affected the page.
	XactReserved   uint16    //50-52 	Number of bytes reserved by the most recently started transaction
	XdeslDPart2    uint32    //52-54
	XdeslIDPart1   uint16    //54-58
	GhostRecCnt    uint16    //58-60
	TornBits       int32     //60-64 bit string 1 bit -> sector
	Reserved       [32]byte  //64-96
}

func (header Header) isValid() bool {

	for typeId := range PageTypes {
		if typeId == header.Type {
			return true
		}
	}
	mslogger.Mslogger.Warning(fmt.Sprintf("Page Id %d Unknown page type %d", header.PageId, header.Type))
	return false
}

func (header Header) GetMetadataAllocUnitId() uint64 {
	return uint64(header.IndexId)<<48 | uint64(header.ObjectId)<<16
}

func (header Header) sanityCheck() bool {
	if header.Version != 1 {

		mslogger.Mslogger.Warning(fmt.Sprintf("Issue with header version %d \n", header.Version))
		return false
	}
	if header.SlotCnt > 4096 {
		mslogger.Mslogger.Warning(fmt.Sprintf("number of slots exceeded maximum allowed number %d.", header.SlotCnt))
		return false
	}
	if header.FreeData > 8192-32 { // not sure
		mslogger.Mslogger.Warning(fmt.Sprintf("Header free area exceeded max allowed size %d", header.FreeData))

	}

	return true
}
