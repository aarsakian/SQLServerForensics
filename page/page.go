package page

import (
	"MSSQLParser/logger"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"bytes"
	"fmt"
	"reflect"
	"sort"
)

var PAGELEN = uint16(8192)
var HEADERLEN = uint16(96)

type InvalidPageTypeError string

func (e InvalidPageTypeError) Error() string { return string(e) }

type InvalidPageSanityError string

func (e InvalidPageSanityError) Error() string { return string(e) }

type ZeroPageHeader string

func (e ZeroPageHeader) Error() string { return string(e) }

var PageTypes = map[uint8]string{
	1: "DATA", 2: "Index", 3: "LOB", 4: "TEXT", 6: "Work File", 7: "Sort", 8: "GAM", 9: "SGAM",
	10: "IAM", 11: "PFS", 13: "Boot", 14: "Server Configuration", 15: "File Header",
	16: "Differential Changed Map", 17: "Buck Change Map",
}

var SystemTablesFlags = map[string]uint32{
	"syscolpars": 0x00000029, "sysrowsets": 0x00000005, "sysiscols": 0x00000037,
	"sysallocationunits": 0x00000007, "sysidxstats": 0x000036,
	"sysschobjs": 0x00000022, "sysrscols": 0x00000003}

type Pages []Page

type SortedPagesByLSN []Page

func (p SortedPagesByLSN) Len() int {
	return len(p)
}

func (p SortedPagesByLSN) Less(i, j int) bool {
	return !p[i].Header.LSN.IsGreaterEqual(p[j].Header.LSN)
}

func (p SortedPagesByLSN) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p Pages) Len() int {
	return len(p)

}

func (p Pages) Less(i, j int) bool {
	return p[i].Header.PageId < p[j].Header.PageId
}

func (p Pages) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type PagesPerIdNodeList struct {
	head *PagesPerIdNode
}

func (pagesPerIDNodeList *PagesPerIdNodeList) UpdateNext(pagesPerIDNode *PagesPerIdNode) {
	node := pagesPerIDNodeList.head

	for node.Next != nil {
		node = node.Next

	}
	node.Next = pagesPerIDNode
}

type PageKey interface {
	uint64 | uint32
}

type PagesPerIdNode struct {
	Next  *PagesPerIdNode
	Pages Pages
}

type PagesPerId[K PageKey] struct {
	Lookup map[K]*PagesPerIdNode
	list   *PagesPerIdNodeList
}

func (pagesPerID PagesPerId[K]) IsEmpty() bool {
	return pagesPerID.list == nil
}

func (pagesPerID PagesPerId[K]) GetHeadNode() *PagesPerIdNode {
	return pagesPerID.list.head
}

func (pagesPerID *PagesPerId[K]) Add(allocUnitID K, page Page) {
	var pagesPerIDNode *PagesPerIdNode
	pagesPerIDNode, ok := pagesPerID.Lookup[allocUnitID]

	if !ok { // new node must be created
		pagesPerIDNode = new(PagesPerIdNode)
		if pagesPerID.list == nil { // first addition
			pagesPerID.Lookup = map[K]*PagesPerIdNode{}
			pagesPerID.list = &PagesPerIdNodeList{head: pagesPerIDNode}
			pagesPerID.list.head = pagesPerIDNode
		} else {
			pagesPerID.list.UpdateNext(pagesPerIDNode)
		}
		pagesPerIDNode.Pages = Pages{page}
		pagesPerID.Lookup[allocUnitID] = pagesPerIDNode

	} else {
		pagesPerIDNode.Pages = append(pagesPerIDNode.Pages, page)

	}

}

func (pagesPerID PagesPerId[K]) GetPages(allocUnitID K) Pages {
	node, ok := pagesPerID.Lookup[allocUnitID]
	if ok {
		return node.Pages
	} else {
		return Pages{}
	}

}

func (pagesPerID PagesPerId[K]) GetFirstPage(allocUnitID K) Page {
	node, ok := pagesPerID.Lookup[allocUnitID]
	if ok {
		return node.Pages[0]
	} else {
		return Page{}
	}

}

type Page struct {
	Header             Header
	Slots              Slots
	DataRows           DataRows
	ForwardingPointers ForwardingPointers
	LOBS               LOBS
	PFSPage            *PFSPage
	GAMExtents         *GAMExtents
	SGAMExtents        *SGAMExtents
	IAMExtents         *IAMExtents
	PrevPage           *Page
	NextPage           *Page
	IndexRows          IndexRows
	Boot               *Boot
	FileHeader         *FileHeader
}

type AllocationMaps interface {
	FilterByAllocationStatus(bool) AllocationMaps
	ShowAllocations()
	GetAllocationStatus([]uint32) string
}

func (header Header) getIndexType() string {
	if header.IndexId == 1 {
		return "Clustered"
	} else if header.IndexId >= 2 && header.IndexId <= 250 || header.IndexId >= 256 && header.IndexId <= 1005 {
		return "Heap"
	} else {
		msg := fmt.Sprintf("index %d reserved from sql number", header.IndexId)
		mslogger.Mslogger.Warning(msg)
		return msg
	}

}

func (page Page) FilterByTable(tablename string) DataRows {
	return utils.Filter(page.DataRows, func(datarow DataRow) bool {
		return datarow.SystemTable.GetName() == tablename

	})

}

func (page Page) GetNextPage() uint32 {
	return page.Header.NextPage
}

func (page Page) GetPrevPage() uint32 {
	return page.Header.PrevPage
}

func (page Page) GetIndexType() string {
	return page.Header.getIndexType()
}

func (page Page) Contains(pageIds []int) bool {
	for _, pageId := range pageIds {
		if page.Header.PageId == uint32(pageId) {
			return true
		}
	}
	return false
}

func (pages Pages) HasPage(pageId int) bool {
	for _, page := range pages {
		if page.Header.PageId == uint32(pageId) {
			return true
		}
	}
	return false

}

func (pages Pages) FilterByTypeToMap(pageType string) PagesPerId[uint32] {
	newpagesPerID := PagesPerId[uint32]{}
	pagesPerType := utils.Filter(pages, func(page Page) bool {
		return page.GetType() == pageType
	})
	for _, page := range pagesPerType {
		newpagesPerID.Add(page.Header.PageId, page)
	}
	return newpagesPerID
}

func (pagesPerID PagesPerId[K]) GetIDs() []K {
	var unitIDs []K
	for unitID := range pagesPerID.Lookup {
		unitIDs = append(unitIDs, unitID)

	}
	return unitIDs

}
func (pagesPerID PagesPerId[K]) FilterByType(pageType string) PagesPerId[K] {
	newpagesPerID := PagesPerId[K]{}

	for allocUnitId, pagesPerIDNode := range pagesPerID.Lookup {
		for _, page := range pagesPerIDNode.Pages {
			if page.GetType() != pageType {
				continue
			}
			newpagesPerID.Add(allocUnitId, page)
		}

	}
	return newpagesPerID

}

func (pagesPerID PagesPerId[K]) FilterByIDSortedByInput(indexPageIDs []uint32) PagesPerId[K] {
	newpagesPerID := PagesPerId[K]{}

	for _, pageId := range indexPageIDs {
		for allocUnitId, pagesPerIDNode := range pagesPerID.Lookup {
			for _, page := range pagesPerIDNode.Pages {

				if page.Header.PageId != uint32(pageId) {
					continue
				}
				newpagesPerID.Add(allocUnitId, page)
			}
		}
	}

	return newpagesPerID
}

func (pagesPerID PagesPerId[K]) FilterByID(pageIDs []int) PagesPerId[K] {
	newpagesPerID := PagesPerId[K]{}
	for allocUnitId, pagesPerIDNode := range pagesPerID.Lookup {
		for _, page := range pagesPerIDNode.Pages {
			if !page.Contains(pageIDs) {
				continue
			}
			newpagesPerID.Add(allocUnitId, page)
		}

	}
	return newpagesPerID
}

func (pagesPerID PagesPerId[K]) FilterBySystemTablesToList(systemTable string) Pages {
	var pages Pages
	pagesPerIDNode := pagesPerID.list.head

	for pagesPerIDNode != nil {
		pages = append(pages, utils.Filter(pagesPerIDNode.Pages, func(page Page) bool {
			return page.isSystemPage(systemTable)
		})...)

		pagesPerIDNode = pagesPerIDNode.Next
	}
	return pages

}

func (pagesPerID PagesPerId[K]) FilterBySystemTables(systemTable string) PagesPerId[K] {

	newpagesPerID := PagesPerId[K]{}

	for allocUnitId, pagesPerIDNode := range pagesPerID.Lookup {
		for _, page := range pagesPerIDNode.Pages {
			if !page.isSystemPage(systemTable) {
				continue
			}
			newpagesPerID.Add(allocUnitId, page)
		}

	}
	return newpagesPerID

}

func (page Page) isSystemPage(systemTable string) bool {
	if systemTable == "all" {
		return page.Header.ObjectId == 0x22 ||
			page.Header.ObjectId == 0x37 || //sysiscols,
			page.Header.ObjectId == 0x05 || //sysrowsets, and
			page.Header.ObjectId == 0x07 //sysallocationunits
	} else {
		return page.Header.ObjectId == SystemTablesFlags[systemTable]
	}
}

func (page Page) GetLobData(lobPages PagesPerId[uint32], textLobPages PagesPerId[uint32], SlotNumber uint, textTimestamp uint) []byte {

	var dataParts [][]byte
	for _, lob := range page.LOBS {

		if lob.Id != uint64(textTimestamp) {
			continue
		}

		dataParts = lob.walk(lobPages, textLobPages, dataParts, textTimestamp, page.Header.PageId)

	}
	return bytes.Join(dataParts, []byte{})
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
	for idx, entry := range data[int(page.Slots[1].Offset)+GAMLen : page.Header.FreeData] {

		for i := 0; i < 8; i++ {

			gamExtents = append(gamExtents, GAMExtent{extent: i + idx*8,
				allocated: entry>>i&1 == 0})

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

func (page *Page) parseLOB(data []byte) {
	var lobs []LOB
	for _, slot := range page.Slots {
		if slot.Offset+14 > PAGELEN {
			mslogger.Mslogger.Info(fmt.Sprintf("Cannot parse LOB slot.Offset exceeds page size by %d\n",
				slot.Offset+14-PAGELEN))
			continue
		}

		var lob *LOB = new(LOB)
		utils.Unmarshal(data[slot.Offset:slot.Offset+14], lob) // 14 byte lob header

		if slot.Offset+lob.Length > PAGELEN {
			mslogger.Mslogger.Info(fmt.Sprintf("Cannot parse LOB LOB length too large it exceeds page  %d\n",
				lob.Length))
			continue
		}
		switch lob.Type {
		case 3: // data leaf
			content := make([]byte, slot.Offset+lob.Length-(slot.Offset+14))
			copy(content, data[slot.Offset+14:slot.Offset+lob.Length])
			lob.Data = content
		case 5: // lob root
			lob.ParseRoot(data[slot.Offset+14 : slot.Offset+lob.Length])
		case 2: //internal
			lob.ParseInternal(data[slot.Offset+14 : slot.Offset+lob.Length])
		}
		lobs = append(lobs, *lob)
	}
	page.LOBS = lobs

}

func (page *Page) parseDATA(data []byte, offset int, carve bool) {
	var allocatedDataRowSize,
		actualDataRowSize,
		slotOffset uint16

	//sorted by offset
	for slotnum, slot := range page.Slots {

		forwardingPointer := new(ForwardingPointer)
		dataRow := &DataRow{Carved: false}

		msg := fmt.Sprintf("%d datarow at %d", slot.Order, offset+int(slot.Offset))
		mslogger.Mslogger.Info(msg)

		//heuristics
		if slot.Offset == 0 {
			msg := "slot.Offset is zero  potential deleted datarow"
			mslogger.Mslogger.Info(msg)
			page.Slots[slotnum].Deleted = true
			if slotnum > 0 {
				page.Slots[slotnum].Offset = page.Slots[slotnum-1].Offset + page.Slots[slotnum-1].AllocatedDataRowSize

			}
			if slotnum < reflect.ValueOf(page.Slots).Len()-1 {
				page.Slots[slotnum].AllocatedDataRowSize = page.Slots[slotnum+1].Offset
			} else if slotnum == reflect.ValueOf(page.Slots).Len()-1 {
				page.Slots[slotnum].AllocatedDataRowSize = page.Header.FreeData - page.Slots[slotnum].Offset
			}
			continue
		} else if slot.Offset < HEADERLEN { //offset starts from 96
			msg := fmt.Sprintf("slot.Offset %d cannot be less than the header size (96B)", slot.Offset)
			mslogger.Mslogger.Info(msg)
			continue
		}

		if page.Header.FreeData < uint16(slot.Offset) {
			msg := fmt.Sprintf(" slot offset %d exceeds free area  %d ", slot.Offset, page.Header.FreeData)
			mslogger.Mslogger.Warning(msg)
			break
		}

		if slotnum+1 < reflect.ValueOf(page.Slots).Len() { //not last one
			allocatedDataRowSize = page.Slots[slotnum+1].Offset - slot.Offset //find allocated legnth
		} else { //last slot
			allocatedDataRowSize = page.Header.FreeData - slot.Offset
		}
		switch GetRowType(data[slot.Offset]) {
		case "Forwarding Record": // forward pointer header
			utils.Unmarshal(data[slot.Offset:slot.Offset+uint16(allocatedDataRowSize)],
				forwardingPointer)
			page.ForwardingPointers = append(page.ForwardingPointers, *forwardingPointer)

		case "Primary Record":
			actualDataRowSize = uint16(dataRow.Parse(data[slot.Offset:slot.Offset+allocatedDataRowSize],
				int(slot.Offset)+offset, page.Header.ObjectId))

			page.DataRows = append(page.DataRows, *dataRow)

			page.Slots[slotnum].ActualDataRowSize = actualDataRowSize
			page.Slots[slotnum].AllocatedDataRowSize = allocatedDataRowSize
		}
		slotOffset += slot.Offset

		if actualDataRowSize != allocatedDataRowSize {
			msg := fmt.Sprintf(" actual row size %d is less than allocated %d", actualDataRowSize, allocatedDataRowSize)
			mslogger.Mslogger.Warning(msg)
		}

	}

	if carve {
		page.CarveData(data, offset)
	}
}

func (pagesPerID PagesPerId[K]) GetAllPages() Pages {

	node := pagesPerID.list.head
	pages := node.Pages
	for node.Next != nil {
		pages = append(pages, node.Pages...)
		node = node.Next

	}
	return pages
}

func (page *Page) CarveData(data []byte, offset int) {
	actualDataRowSize := uint16(0)
	slotOffset := HEADERLEN
	//returns slice copies pointer need to copy values

	sort.Sort(SortedSlotsByOrder(page.Slots))

	//// this section is experimental
	// found area that is unallocated?
	//calculate size of unallocate cols

	slackOffset := uint16(0)

	for slotnum, slot := range page.Slots {

		if slot.Deleted { //slot offset is zero get previous allocated size or set to 96 page header size
			if slotnum > 0 {
				slotOffset = page.Slots[slotnum-1].Offset +
					page.Slots[slotnum-1].AllocatedDataRowSize
			}

		} else {
			slotOffset = slot.Offset + slot.ActualDataRowSize

		}

		if actualDataRowSize > PAGELEN-uint16(2*len(page.Slots)) ||
			slotOffset > PAGELEN-uint16(2*len(page.Slots)) ||
			slot.AllocatedDataRowSize > PAGELEN-uint16(2*len(page.Slots)) {
			continue
		}

		slackSpace := slot.AllocatedDataRowSize - slot.ActualDataRowSize
		// if slot has slack carve and has available space
		for slackOffset < slackSpace && slotOffset+slackOffset < PAGELEN-uint16(2*len(page.Slots)) {

			// accept only primary records

			if GetRowType(data[slotOffset+slackOffset]) == "Ghost Record" {
				slotnum += 1 //extra slot recovered
				dataRow := DataRow{Carved: true}
				actualDataRowSize = uint16(dataRow.Parse(
					data[slotOffset+slackOffset:],
					int(slotOffset)+int(slackOffset)+offset, page.Header.ObjectId))
				page.DataRows = append(page.DataRows, dataRow)
				if actualDataRowSize == 0 {
					logger.Mslogger.Info("carved actualDataRowSize is zero")
					break
				}
				slackOffset += actualDataRowSize
			} else {
				slackOffset += 1
			}

		}

	}

}

func (page *Page) parseSGAM(data []byte) {
	var sgamExtents SGAMExtents
	SGAMLen := 4
	for idx, entry := range data[int(page.Slots[1].Offset)+SGAMLen : page.Header.FreeData] {

		for i := 0; i < 8; i++ {

			sgamExtents = append(sgamExtents, SGAMExtent{i + idx*8, entry>>i&1 == 0})

		}

	}
	page.SGAMExtents = &sgamExtents
}

func (page *Page) parsePFS(data []byte) {
	var pfsPage PFSPage
	for idx, entry := range data[page.Slots[0].Offset:page.Header.FreeData] {
		pfsPage = append(pfsPage, PFS{uint32(idx), PFSStatus[uint8(entry)]})
	}

	page.PFSPage = &pfsPage
}

func (page Page) PrintHeader(showSlots bool) {
	header := page.Header
	header.Print()
	if showSlots {

		page.printSlots()

	}
}

func (page Page) printSlots() {
	fmt.Printf("Slots offsets: ")
	for _, slot := range page.Slots {
		fmt.Printf("%d ", slot.Offset)
	}
	fmt.Printf("\n")
}

func (page Page) ShowCarvedDataRows() {
	for _, datarow := range page.DataRows {
		if datarow.Carved {
			datarow.ShowData()
		}

	}
}

func (page Page) ShowRowData() {

	for _, datarow := range page.DataRows {
		if datarow.Carved {
			continue
		}
		datarow.ShowData()
	}
}

func (page Page) ShowIndexRows() {
	for idx, indexrow := range page.IndexRows {
		fmt.Printf("row %d ", idx)
		indexrow.ShowData()
	}
}

func (page Page) ShowSlotInfo() {

	fmt.Printf("Slots info page %d\n", page.Header.PageId)
	for slotnum, slot := range page.Slots {
		fmt.Printf("%d: offset %d slack area %d\n", slotnum,
			slot.Offset, slot.AllocatedDataRowSize-slot.ActualDataRowSize)
	}
}

func (page *Page) parseIAM(data []byte) {
	var iams IAMExtents
	for idx, entry := range data[page.Slots[1].Offset:page.Header.FreeData] {
		for i := 0; i < 8; i++ {
			iams = append(iams, IAMExtent{i + idx*8, entry>>i&1 == 0})
		}
	}

	page.IAMExtents = &iams
}

func (page *Page) parseIndex(data []byte, offset int) {
	page.IndexRows = make(IndexRows, len(page.Slots))

	for slotnum, slot := range page.Slots {
		msg := fmt.Sprintf("%d index row at %d", slotnum, offset+int(slot.Offset))
		mslogger.Mslogger.Info(msg)

		if slot.Offset < 96 { //offset starts from 96
			msg := fmt.Sprintf("slot.Offset %d less than header size \n", slot.Offset)
			mslogger.Mslogger.Info(msg)
			continue
		}

		var indexRowLen uint16

		if slotnum+1 < reflect.ValueOf(page.Slots).Len() { //not last one
			indexRowLen = page.Slots[slotnum+1].Offset - slot.Offset //find legnth
		} else if page.Header.FreeData < uint16(slot.Offset) {
			msg := fmt.Sprintf("skipping free area starts before slot offset %d %d ", page.Header.FreeData, slot.Offset)
			mslogger.Mslogger.Warning(msg)
			continue
		} else { //last slot
			indexRowLen = page.Header.FreeData - slot.Offset
		}

		if int(indexRowLen+slot.Offset) >= len(data) {
			msg := fmt.Sprintf("exceeded buffer length %d by %d", len(data), len(data)-int(indexRowLen+slot.Offset))
			mslogger.Mslogger.Warning(msg)
			break
		}

		indexRow := new(IndexRow)
		indexRow.Parse(data[slot.Offset:slot.Offset+indexRowLen], offset+int(slot.Offset), page.Header.PMinLen)

		/*dst := make([]byte, page.Header.PMinLen-1)                                     // allocate memory for fixed len cols
		copy(dst, data[slot.Offset+1:slot.Offset+Slots(page.Header.PMinLen)]) //first always statusA
		indexRow.FixedLenCols = dst
		if utils.HasNullBitmap(data[slot.Offset]) {
			utils.Unmarshal(data[slot.Offset:slot.Offset+indexRowLen], indexRow)

			indexRow.ProcessVaryingCols(data[slot.Offset:], offset+int(slot.Offset))

		}*/

		page.IndexRows[slot.Order] = *indexRow

	}

}

func (page *Page) parseBoot(data []byte) {

	slot := page.Slots[0] //only one slot at boot page

	boot := new(Boot)

	utils.Unmarshal(data[slot.Offset+4:], boot)
	page.Boot = boot
}

func (page *Page) parseFileHeader(data []byte) {
	//Svar fileHeader *FileHeader
	fileHeader := new(FileHeader)
	utils.Unmarshal(data[page.Slots[0].Offset:], fileHeader)
	page.FileHeader = fileHeader
}

func (page *Page) Process(data []byte, offset int, carve bool) error {
	HEADERLEN := 96

	var header Header
	if utils.IsZeroed(data[:HEADERLEN]) {
		return ZeroPageHeader("Zero page header")
	}
	utils.Unmarshal(data[0:HEADERLEN], &header)

	if !header.isValid() {
		return InvalidPageTypeError("Invalid page header")
	} else if !header.sanityCheck() {
		return InvalidPageSanityError(fmt.Sprintf("Page %d failed sanity checks", header.PageId))
	}

	page.Header = header
	mslogger.Mslogger.Info(fmt.Sprintf("Page Header OK Id %d Type %s Object Id %d nof slots %d",
		header.PageId, page.GetType(), page.Header.ObjectId, page.Header.SlotCnt))

	page.PopulateSlots(data[PAGELEN-2*header.SlotCnt:])

	if len(page.Slots) != int(header.SlotCnt) {
		mslogger.Mslogger.Info(fmt.Sprintf("Discrepancy in number of page slots declared %d actual %d",
			header.SlotCnt, len(page.Slots)))
	}

	switch page.GetType() {
	case "PFS":
		page.parsePFS(data)
	case "GAM":
		page.parseGAM(data)
	case "SGAM":
		page.parseSGAM(data)
	case "DATA":
		page.parseDATA(data, offset, carve)
	case "LOB":
		page.parseLOB(data)
	case "TEXT":
		page.parseLOB(data)
	case "Index":
		page.parseIndex(data, offset)
	case "IAM":
		page.parseIAM(data)
	case "File Header":
		page.parseFileHeader(data)
	case "Boot":
		page.parseBoot(data)
	}
	return nil
}

func (page *Page) PopulateSlots(data []byte) {
	slots := retrieveSlots(data) //starts from end of page
	sort.Sort(SortedSlotsByOffset(slots))
	page.Slots = slots

}

func (page Page) HasForwardingPointers() bool {
	return len(page.ForwardingPointers) != 0
}

func (page Page) FollowForwardingPointers() []uint32 {
	var pagesIds []uint32
	for _, forwardingPointer := range page.ForwardingPointers {
		pageId := forwardingPointer.RowId.PageId
		pagesIds = append(pagesIds, pageId)
	}
	return pagesIds
}
