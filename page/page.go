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

var SystemTablesFlags = map[string]uint8{
	"syscolpars": 0x29, "sysrowsets": 0x05, "sysiscols": 0x37, "sysallocationunits": 0x07,
	"sysschobjs": 0x22}

var PFSStatus = map[uint8]string{
	0: "NOT ALLOCATED 0PCT_FULL", 8: "NOT ALLOCATED 100PCT_FULL", 68: "ALLOCATED 100FULL",
	96: "ALLOCATED Mixed Extent 0PTC_FULL", 116: "ALLOCATED Mixed Extent IAM 100PCT_FULL",
	112: "ALLOCATED Mixed Extent IAM EMPTY", 64: "ALLOCATED EMPTY", 65: "ALLOCATED 50PCT_FULL",
	66: "ALLOCATED 80PCT_FULL", 67: "ALLOCATED 95PCT_FULL", 156: "UNUSED HAS_GHOST D 100PCT_FULL"}

type Pages map[uint32][]Page

type Page struct {
	Header      Header
	Slots       []utils.SlotOffset
	DataRows    DataRows
	PFSPage     *PFSPage
	GAMExtents  *GAMExtents
	SGAMExtents *SGAMExtents
	IAMExtents  *IAMExtents
	PrevPage    *Page
	NextPage    *Page
}

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
	ObjectId       uint32    //24-28 AllocUnitId.idObj
	FreeCnt        uint16    //28-30 free space in bytes
	FreeData       uint16    //30-32 offset from the start of the page to the first byte after the last record
	PageId         uint32    //32-36
	FragId         uint32    //36-40
	LSN            utils.LSN //40-52
	Unknown5       [8]byte   //52-60
	TornBits       int32     //60-64
	Reserved       [32]byte  //64-96
}

type AllocationMaps interface {
	FilterByAllocationStatus(bool) AllocationMaps
	ShowAllocations()
}

func (page Page) FilterByTable(tablename string) DataRows {
	return utils.Filter(page.DataRows, func(datarow DataRow) bool {
		return datarow.SystemTable.GetName() == tablename

	})

}

func (pages Pages) FilterByType(pageType string) Pages {
	return utils.FilterMap(pages, func(page Page) bool {
		return page.GetType() == pageType
	})
}

func (pages Pages) FilterBySystemTables(systemTable string) Pages {
	return utils.FilterMap(pages, func(page Page) bool {
		if systemTable == "all" {
			return page.Header.ObjectId == 0x22 ||
				page.Header.ObjectId == 0x37 || //sysiscols,
				page.Header.ObjectId == 0x05 || //sysrowsets, and
				page.Header.ObjectId == 0x07 //sysallocationunits
		} else {
			return page.Header.ObjectId == uint32(SystemTablesFlags[systemTable])
		}
	})
}

type SystemTable interface {
	GetName() string
	SetName([]byte)
	ShowData()
	GetData() (any, any)
}

func (dataRow *DataRow) ProcessVaryingCols(data []byte) {

	startVarColOffset := dataRow.GetVarCalOffset()
	endVarColOffset := dataRow.VarLengthColOffsets[0]
	if endVarColOffset > startVarColOffset {
		cpy := make([]byte, endVarColOffset-startVarColOffset)
		copy(cpy, data[startVarColOffset:endVarColOffset])
		if dataRow.SystemTable != nil {
			dataRow.SystemTable.SetName(cpy)
		}

	}

}

func (dataRow *DataRow) ProcessData(datacols *DataCols) {

}

func (dataRow *DataRow) Process(systemtable SystemTable) {
	//nofColsFixedLen := int(dataRow.NumberOfCols - dataRow.NumberOfVarLengthCols)

	utils.Unmarshal(dataRow.FixedLenCols, systemtable)
	dataRow.SystemTable = systemtable
	/*var dataCols DataCols
	for colId := 0; colId < nofColsFixedLen; colId++ {

		if dataRow.NullBitmap>>colId&1 == 1 { //col is NULL skip
			continue
		}

		if colOffset+2 >= len(data) {
			break
		}

		dataCols = append(dataCols, DataCol{uint16(colId), uint16(colOffset), data[colOffset : colOffset+2]}) // fixed size col =2 bytes
		colOffset += 2
	}*/

	/*for colId := 0; colId < int(dataRow.NumberOfVarLengthCols); colId++ {
		if colId+nofColsFixedLen == int(dataRow.NullBitmap&1<<colId) { //col is NULL skip
			continue
		}
		endVarColOffset := dataRow.VarLengthColOffsets[colId]

		dataCols = append(dataCols, DataCol{uint16(colId + nofColsFixedLen),
			startVarColOffset, data[startVarColOffset:endVarColOffset]})
		startVarColOffset = endVarColOffset

	}

	dataRow.DataCols = &dataCols*/

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

func (page Page) ShowGAMStats() {
	allocatedPages, unallocatedPages := page.GAMExtents.GetStats()
	fmt.Printf("GAM allocated %d unallocated %d \n", allocatedPages, unallocatedPages)
}

func (page *Page) parseGAM(data []byte) {
	var gamExtents GAMExtents
	GAMLen := 4
	for idx, entry := range data[int(page.Slots[1])+GAMLen : page.Header.FreeData] {

		for i := 0; i < 8; i++ {

			gamExtents = append(gamExtents, GAMExtent{i + idx*8, entry>>i&1 == 0})

		}

	}
	page.GAMExtents = &gamExtents
}

func (page Page) GetAllocationMaps() AllocationMaps {
	var allocMap AllocationMaps
	if page.GAMExtents != nil {
		allocMap = *page.GAMExtents
	} else if page.SGAMExtents != nil {
		allocMap = *page.SGAMExtents
	} else if page.IAMExtents != nil {
		allocMap = *page.IAMExtents
	} else if page.PFSPage != nil {
		allocMap = *page.PFSPage
	}
	return allocMap
}

func (page *Page) parseDATA(data []byte) {
	var dataRows DataRows
	for slotnum, slotoffset := range page.Slots {
		var dataRowLen utils.SlotOffset
		if slotnum+1 < reflect.ValueOf(page.Slots).Len() {
			dataRowLen = page.Slots[slotnum+1] - slotoffset //find legnth
		} else {
			dataRowLen = 8192 - slotoffset //last slot
		}

		var dataRow *DataRow = new(DataRow)
		utils.Unmarshal(data[slotoffset:slotoffset+dataRowLen], dataRow)
		//	fmt.Println(slotoffset, slotnum, reflect.ValueOf(page.Slots).Len())
		if page.Header.ObjectId == 0x29 { //syscolpars

			var syscolpars *SysColpars = new(SysColpars)

			dataRow.Process(syscolpars)

			if syscolpars.Id == 0x22 || //sysschobjs
				syscolpars.Id == 0x37 || //sysiscols,
				syscolpars.Id == 0x05 || //sysrowsets, and
				syscolpars.Id == 0x07 { //sysallocationunits

				syscolpars.GetType()
			}

		} else if page.Header.ObjectId == 0x22 {

			var sysschobjs *Sysschobjs = new(Sysschobjs)

			dataRow.Process(sysschobjs) // from slot to end

		} else if page.Header.ObjectId == 0x07 {
			var sysallocationunits *SysAllocUnits = new(SysAllocUnits)
			dataRow.Process(sysallocationunits)

		} else if page.Header.ObjectId == 0x05 {
			var sysrowsets *SysRowSets = new(SysRowSets)
			dataRow.Process(sysrowsets)
		} else if page.Header.ObjectId == 0x37 {
			var sysiscols *sysIsCols = new(sysIsCols)
			dataRow.Process(sysiscols)
		} else {
			var datacols *DataCols = new(DataCols)
			dataRow.ProcessData(datacols)
			//	fmt.Printf("%d obj id %x \n", page.Header.PageId, page.Header.ObjectId)
		}

		if dataRow.NumberOfVarLengthCols != 0 {
			dataRow.ProcessVaryingCols(data[slotoffset : slotoffset+dataRowLen])
		}
		dataRows = append(dataRows, *dataRow)
	}
	page.DataRows = dataRows
}

func (page *Page) parseSGAM(data []byte) {
	var sgamExtents SGAMExtents
	SGAMLen := 4
	for idx, entry := range data[int(page.Slots[1])+SGAMLen : page.Header.FreeData] {

		for i := 0; i < 8; i++ {

			sgamExtents = append(sgamExtents, SGAMExtent{i + idx*8, entry>>i&1 == 0})

		}

	}
	page.SGAMExtents = &sgamExtents
}

func (page *Page) parsePFS(data []byte) {
	var pfsPage PFSPage
	for idx, entry := range data[page.Slots[0]:page.Header.FreeData] {
		pfsPage = append(pfsPage, PFS{uint32(idx), PFSStatus[uint8(entry)]})
	}

	page.PFSPage = &pfsPage
}

func (page Page) PrintHeader(showSlots bool) {
	header := page.Header
	if showSlots {

		fmt.Printf("slots %x ", page.Slots)

	}
	fmt.Printf("Page Id %d type %s objectid %d slots %d free space %d Prev page %d  Next page %d \n",
		header.PageId, page.GetType(),
		header.ObjectId, header.SlotCnt, header.FreeData, header.PrevPage, header.NextPage)

}

func (page Page) ShowRowData() {
	for _, datarow := range page.DataRows {
		//datarow.ShowData()
		if datarow.SystemTable != nil {
			datarow.SystemTable.ShowData()
		}

	}
}

func (page *Page) parseIAM(data []byte) {
	var iams IAMExtents
	for idx, entry := range data[page.Slots[1]:page.Header.FreeData] {
		for i := 0; i < 8; i++ {
			iams = append(iams, IAMExtent{i + idx*8, entry>>i&1 == 0})
		}
	}

	page.IAMExtents = &iams
}

func (page *Page) parseIndex(data []byte) {

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
	page.Slots = slotsOffset

	switch page.GetType() {
	case "PFS":
		page.parsePFS(data)
	case "GAM":
		page.parseGAM(data)
	case "SGAM":
		page.parseSGAM(data)
	case "DATA":
		page.parseDATA(data)
	case "Index":
		page.parseIndex(data)
	case "IAM":
		page.parseIAM(data)
	}

	pos := slotsOffset[0]
	for idx, slotOffset := range slotsOffset {
		if idx == 0 {
			continue
		}

		pos += slotOffset
	}
	//	fmt.Printf("%d", PAGELEN-int(page.header.FreeCnt)-int(pos)-2)

}
