package page

import (
	"MSSQLParser/utils"
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
)

var PageTypes = map[uint8]string{
	1: "DATA", 2: "Index", 3: "Text", 4: "Text", 7: "Sort", 8: "GAM", 9: "SGAM",
	10: "IAM", 11: "PFS", 13: "Boot", 15: "File Header",
	16: "Differential Changed Map", 17: "Buck Change Map",
}

var PageTypeFlagBits = map[uint8]string{
	1: "Ghost record", 4: "Fixed size rows",
}

var PFSStatus = map[uint8]string{
	0: "NOT ALLOCATED 0PCT_FULL", 8: "NOT ALLOCATED 100PCT_FULL", 68: "ALLOCATED 100FULL",
	96: "ALLOCATED Mixed Extend 0PTC_FULL", 116: "ALLOCATED Mixed Extend IAM 100PCT_FULL",
	112: "ALLOCATED Mixed Extend IAM EMPTY", 64: "ALLOCATED EMPTY", 65: "ALLOCATED 50PCT_FULL",
	66: "ALLOCATED 80PCT_FULL", 67: "ALLOCATED 95PCT_FULL", 156: "UNUSED HAS_GHOST D 100PCT_FULL"}

type DataCol struct {
	id      uint16
	offset  uint16
	content []byte
}
type DataCols []DataCol



type PFSPage []PFS

type PFS struct {
	pageID uint8
	status string
}

type GAMPages []GAM

type GAM struct {
	extent    int
	allocated bool
}

type SGAMPages []SGAM

type SGAM struct {
	extent    int
	allocated bool
}

type Page struct {
	Header    Header
	slots     []utils.SlotOffset
	dataRows  []DataRow
	PFSPage   *PFSPage
	GAMPages  *GAMPages
	SGAMPages *SGAMPages
}



type Header struct {
	Version  uint8    //1
	Type     uint8    // 1-2
	unknown1 [2]byte  //2-4
	FlagBits [2]byte  //4-6
	unknown2 [8]byte  //6-14
	PMinLen  uint16   //14-16  size of fixed len records
	unknown3 [6]byte  //16-22
	SlotCnt  uint16   //22-24   number of slots (records) that hold data
	ObjectId uint32   //24-28
	FreeCnt  uint16   //28-30 free space in bytes
	FreeData uint16   //30-32 offset from the start of the page to the first byte after the last record
	PageId   uint32   //32-36
	FragId   uint32   //36-40
	LSN      utils.LSN      //40-52
	unknown5 [8]byte  //52-60
	TornBits int32    //60-64
	unknown6 [32]byte //64-96
}

type DataRow struct {
	StatusA               uint8  //1
	StatusB               uint8  //2
	NofColsOffset         uint16 //3-4
	FixedLenCols          []byte
	NumberOfCols          uint16
	NullBitmap            uint8
	NumberOfVarLengthCols uint16
	DataCols              *DataCols
}

type AllocationMaps interface {
	FilterByAllocationStatus(bool) AllocationMaps
	ShowAllocations()
}

func (dataRow DataRow) Len() uint16 {
	return uint16(reflect.ValueOf(dataRow.FixedLenCols).Len() + 9)
}

func (dataRow *DataRow) Process(data []byte) {
	nofColsFixedLen := dataRow.NumberOfCols - dataRow.NumberOfVarLengthCols
	cnt := 1
	var dataCols DataCols
	for colId := uint16(0); colId < nofColsFixedLen; colId++ {
		if dataRow.NullBitmap>>colId&1 == 1 { //col is NULL skip
			continue
		}
		dataCols = append(dataCols, DataCol{colId, colId * 4, data[4*cnt : 4*cnt+4]})
		cnt++
	}

	startVarColOffsets := dataRow.Len()                                 //where var col offsets start
	endVarColOffsets := dataRow.Len() + 2*dataRow.NumberOfVarLengthCols //where var col offsets end
	var endVarColOffset uint16                                          // where each var len col ends
	for colId := uint16(0); colId < dataRow.NumberOfVarLengthCols; colId++ {
		if colId+nofColsFixedLen == uint16(dataRow.NullBitmap&1<<colId) { //col is NULL skip
			continue
		}
		binary.Read(bytes.NewBuffer(data[startVarColOffsets+2*colId:startVarColOffsets+2*colId+2]),
			binary.LittleEndian, &endVarColOffset)
		if colId == 0 {
			dataCols = append(dataCols, DataCol{colId + nofColsFixedLen,
				endVarColOffsets, data[endVarColOffsets:endVarColOffset]})
		}

	}
	dataRow.DataCols = &dataCols

}

func retrieveSlots(data []byte) []utils.SlotOffset {
	var slotsOffset []utils.SlotOffset

	var slot utils.SlotOffset
	for idx := 0; idx < binary.Size(data); idx += 2 {
		binary.Read(bytes.NewBuffer(data[idx:idx+2]), binary.LittleEndian, &slot)
		slotsOffset = append(slotsOffset, slot)

	}
	return slotsOffset
}

func (page Page) GetType() string {
	return PageTypes[page.Header.Type]
}

func (gamPages GAMPages) ShowAllocations() {
	var allocatedPages []int
	pageRange := 0
	for _, gampage := range gamPages {
		if gampage.allocated {

		} else {
			allocatedPages = append(allocatedPages, pageRange)
			fmt.Printf("allocated range %d ", pageRange)

		}
		pageRange += 8
	}

}

func (sgamPages SGAMPages) ShowAllocations() {
	var allocatedPages []int
	pageRange := 0
	for _, sgampage := range sgamPages {
		if sgampage.allocated {

		} else {
			allocatedPages = append(allocatedPages, pageRange)
			fmt.Printf("allocated range %d ", pageRange)
		}
		pageRange += 8
	}
}

func (gamPages GAMPages) FilterByAllocationStatus(status bool) AllocationMaps {
	return GAMPages(utils.Filter(gamPages, func(gam GAM) bool {
		return gam.allocated == status
	}))

}

func (sgamPages SGAMPages) FilterByAllocationStatus(status bool) AllocationMaps {
	return SGAMPages(utils.Filter(sgamPages, func(sgam SGAM) bool {
		return sgam.allocated == status
	}))

}

func (gamPages GAMPages) GetStats() (int, int) {
	allocatedGamPages := gamPages.FilterByAllocationStatus(true)
	unallocatedGamPages := gamPages.FilterByAllocationStatus(false)
	return reflect.ValueOf(allocatedGamPages).Len() * 8,
		reflect.ValueOf(unallocatedGamPages).Len() * 8

}

func (page Page) ShowGAMStats() {
	allocatedPages, unallocatedPages := page.GAMPages.GetStats()
	fmt.Printf("GAM allocated %d unallocated %d \n", allocatedPages, unallocatedPages)
}

func (page *Page) parseGAM(data []byte) {
	var gamPages GAMPages
	GAMLen := 4
	for idx, entry := range data[int(page.slots[1])+GAMLen : page.Header.FreeData] {

		for i := 0; i < 8; i++ {

			gamPages = append(gamPages, GAM{i + idx*8, entry>>i&1 == 0})

		}

	}
	page.GAMPages = &gamPages
}

func (page Page) GetAllocationMaps() AllocationMaps {
	var allocMap AllocationMaps
	if page.GAMPages != nil {
		allocMap = *page.GAMPages
	} else if page.SGAMPages != nil {
		allocMap = *page.SGAMPages
	}
	return allocMap
}

func (page *Page) parseDATA(data []byte) {
	var dataRows []DataRow
	for _, slotoffset := range page.slots {
		var dataRow DataRow
		utils.Unmarshal(data[slotoffset:], &dataRow)

		dataRow.Process(data[slotoffset:]) // from slot to end
		dataRows = append(dataRows, dataRow)
	}
	page.dataRows = dataRows
}

func (page Page) showData() {
	for slotId, dataRow := range page.dataRows {
		fmt.Printf("Slot %d Record size offset %x \n", slotId, page.slots[slotId])
		for _, dataCol := range *dataRow.DataCols {
			fmt.Printf("col id %d offset %x len %d \n",
				dataCol.id, dataCol.offset, reflect.ValueOf(dataCol.content).Len())
		}

	}
}

func (page *Page) parseSGAM(data []byte) {
	var sgamPages SGAMPages
	SGAMLen := 4
	for idx, entry := range data[int(page.slots[1])+SGAMLen : page.Header.FreeData] {

		for i := 0; i < 8; i++ {

			sgamPages = append(sgamPages, SGAM{i + idx*8, entry>>i&1 == 0})

		}

	}
	page.SGAMPages = &sgamPages
}

func (sgamPages SGAMPages) GetStats() (int, int) {
	allocatedGamPages := sgamPages.FilterByAllocationStatus(true)
	unallocatedGamPages := sgamPages.FilterByAllocationStatus(false)
	return reflect.ValueOf(allocatedGamPages).Len() * 8,
		reflect.ValueOf(unallocatedGamPages).Len() * 8

}

func (page *Page) parsePFS(data []byte) {
	var pfsPage PFSPage
	for _, entry := range data[page.slots[0]:page.Header.FreeData] {
		pfsPage = append(pfsPage, PFS{uint8(entry), PFSStatus[uint8(entry)]})
	}

	page.PFSPage = &pfsPage
}

func (page *Page) Process(data []byte) {
	HEADERLEN := 96
	PAGELEN := 8192
	var header Header
	utils.Unmarshal(data[0:HEADERLEN], &header)
	page.Header = header
	if page.Header.Type == 0 {
		return
	}
	slotsOffset := retrieveSlots(data[PAGELEN-int(2*header.SlotCnt):])
	sort.Sort(utils.SortedSlotsOffset(slotsOffset))
	page.slots = slotsOffset

	switch page.GetType() {
	case "PFS":
		page.parsePFS(data)
	case "GAM":
		page.parseGAM(data)
	case "SGAM":
		page.parseSGAM(data)
	case "DATA":
		page.parseDATA(data)
		page.showData()
	}

	pos := slotsOffset[0]
	for idx, slotOffset := range slotsOffset {
		if idx == 0 {
			continue
		}

		//data[pos : pos+nextlot]

		pos += slotOffset
	}
	//	fmt.Printf("%d", PAGELEN-int(page.header.FreeCnt)-int(pos)-2)

}
