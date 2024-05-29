package page

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
)

var PAGELEN = 8192

var PageTypes = map[uint8]string{
	1: "DATA", 2: "Index", 3: "LOB", 4: "TEXT", 6: "Work File", 7: "Sort", 8: "GAM", 9: "SGAM",
	10: "IAM", 11: "PFS", 13: "Boot", 14: "Server Configuration", 15: "File Header",
	16: "Differential Changed Map", 17: "Buck Change Map",
}

var SystemTablesFlags = map[string]int32{
	"syscolpars": 0x00000029, "sysrowsets": 0x00000005, "sysiscols": 0x00000037,
	"sysallocationunits": 0x00000007, "sysidxstats": 0x000036,
	"sysschobjs": 0x00000022, "sysrscols": 0x00000003}

type Pages []Page

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
	Slots              []utils.SlotOffset
	DataRows           DataRows
	CarvedDataRows     DataRows
	ForwardingPointers ForwardingPointers
	LOBS               LOBS
	PFSPage            *PFSPage
	GAMExtents         *GAMExtents
	SGAMExtents        *SGAMExtents
	IAMExtents         *IAMExtents
	PrevPage           *Page
	NextPage           *Page
	IndexRows          IndexRows
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

type AllocationMaps interface {
	FilterByAllocationStatus(bool) AllocationMaps
	ShowAllocations()
	GetAllocationStatus(uint32) string
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

func retrieveSlots(data []byte) []utils.SlotOffset {
	var slotsOffset []utils.SlotOffset

	var slot utils.SlotOffset
	for idx := 0; idx < binary.Size(data); idx += 2 {
		binary.Read(bytes.NewBuffer(data[idx:idx+2]), binary.LittleEndian, &slot)
		if slot == 0 {
			continue
		}
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
	for _, slotoffset := range page.Slots {
		if slotoffset+14 > utils.SlotOffset(PAGELEN) {
			mslogger.Mslogger.Info(fmt.Sprintf("Cannot parse LOB slotoffset exceeds page size by %d\n",
				slotoffset+14-utils.SlotOffset(PAGELEN)))
			continue
		}

		var lob *LOB = new(LOB)
		utils.Unmarshal(data[slotoffset:slotoffset+14], lob) // 14 byte lob header

		if slotoffset+utils.SlotOffset(lob.Length) > utils.SlotOffset(PAGELEN) {
			mslogger.Mslogger.Info(fmt.Sprintf("Cannot parse LOB LOB length too large it exceeds page  %d\n",
				utils.SlotOffset(lob.Length)))
			continue
		}
		if lob.Type == 3 { // data leaf
			content := make([]byte, slotoffset+utils.SlotOffset(lob.Length)-(slotoffset+14))
			copy(content, data[slotoffset+14:slotoffset+utils.SlotOffset(lob.Length)])
			lob.Data = content
		} else if lob.Type == 5 { // lob root
			lob.ParseRoot(data[slotoffset+14 : slotoffset+utils.SlotOffset(lob.Length)])
		} else if lob.Type == 2 { //internal
			lob.ParseInternal(data[slotoffset+14 : slotoffset+utils.SlotOffset(lob.Length)])
		}
		lobs = append(lobs, *lob)
	}
	page.LOBS = lobs

}

func (page *Page) parseDATA(data []byte, offset int, carve bool) {

	for slotnum, slotoffset := range page.Slots {
		var allocatedDataRowSize utils.SlotOffset
		var forwardingPointer *ForwardingPointer = new(ForwardingPointer)
		var dataRow *DataRow = new(DataRow)

		var actualDataRowSize int

		msg := fmt.Sprintf("%d datarow at %d", slotnum, offset+int(slotoffset))
		mslogger.Mslogger.Info(msg)
		if slotoffset == 0 {
			msg := "slotoffset is zero  potential deleted datarow \n"
			mslogger.Mslogger.Info(msg)
			continue
			//heuristics

		} else if slotoffset < 96 { //offset starts from 96
			msg := fmt.Sprintf("slotoffset %d cannot be less than 96 bytes \n", slotoffset)
			mslogger.Mslogger.Info(msg)
			continue
		}

		if page.Header.FreeData < uint16(slotoffset) {
			msg := fmt.Sprintf("skipping free area starts before slot offset %d %d ", page.Header.FreeData, slotoffset)
			mslogger.Mslogger.Warning(msg)
			continue
		}

		if slotnum+1 < reflect.ValueOf(page.Slots).Len() { //not last one
			allocatedDataRowSize = page.Slots[slotnum+1] - slotoffset //find allocated legnth
		} else { //last slot
			allocatedDataRowSize = utils.SlotOffset(page.Header.FreeData) - slotoffset
		}

		if GetRowType(data[slotoffset]) == "Forwarding Record" { // forward pointer header
			utils.Unmarshal(data[slotoffset:slotoffset+allocatedDataRowSize],
				forwardingPointer)
			page.ForwardingPointers = append(page.ForwardingPointers, *forwardingPointer)

		} else if GetRowType(data[slotoffset]) == "Primary Record" {
			actualDataRowSize = dataRow.Parse(data[slotoffset:slotoffset+allocatedDataRowSize],
				int(slotoffset)+offset, page.Header.ObjectId)

			page.DataRows = append(page.DataRows, *dataRow)
		}
		//// this section is experimental
		// found area that is unallocated?
		unallocatedArea := allocatedDataRowSize - utils.SlotOffset(actualDataRowSize)
		if unallocatedArea > 0 && carve {
			//calculate size of unallocate cols
			slotoffset += utils.SlotOffset(actualDataRowSize) // add last row size
			//carve only when there is unallocated space in datarow
			if slotoffset < utils.SlotOffset(page.Header.FreeData) &&
				slotoffset+unallocatedArea <= utils.SlotOffset(page.Header.FreeData) {
				page.CarveDataRows(data[slotoffset:slotoffset+unallocatedArea], offset+int(slotoffset))
			}

		}

	}

}

func (page *Page) CarveDataRows(unallocatedData []byte, offset int) {
	var carvedDataRow *DataRow = new(DataRow)
	slotoffset := 0
	for slotoffset < len(unallocatedData) && slotoffset > 0 {
		// second condition for negative offsets in var cols offsets

		msg := fmt.Sprintf("unallocated space discovered at %d",
			offset+len(unallocatedData))
		mslogger.Mslogger.Warning(msg)
		if GetRowType(unallocatedData[slotoffset]) != "Primary Record" {
			break
		}
		dataRowSize := carvedDataRow.Parse(unallocatedData[slotoffset:],
			int(slotoffset)+offset, page.Header.ObjectId)
		slotoffset += dataRowSize

		page.CarvedDataRows = append(page.CarvedDataRows, *carvedDataRow)

	}
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
	fmt.Printf("Metadata AllocUnitId %d  \n",
		header.GetMetadataAllocUnitId())

	fmt.Printf("Page Id %d type %s objectid %d index %d, slots %d free space %d Prev page %d  Next page %d \n",
		header.PageId, page.GetType(), header.ObjectId, header.IndexId,
		header.SlotCnt, header.FreeData, header.PrevPage, header.NextPage)

	if showSlots {

		page.printSlots()

	}
}

func (page Page) printSlots() {
	fmt.Printf("Slots offsets: ")
	for _, slot := range page.Slots {
		fmt.Printf("%d ", slot)
	}
}

func (page Page) ShowCarvedDataRows() {
	for _, datarow := range page.CarvedDataRows {
		datarow.ShowData()
	}
}

func (page Page) ShowRowData() {

	for _, datarow := range page.DataRows {

		datarow.ShowData()
	}
}

func (page Page) ShowIndexRows() {
	for idx, indexrow := range page.IndexRows {
		fmt.Printf("row %d ", idx)
		indexrow.ShowData()
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

func (page *Page) parseIndex(data []byte, offset int) {
	var indexRows IndexRows
	for slotnum, slotoffset := range page.Slots {
		msg := fmt.Sprintf("%d index row at %d", slotnum, offset+int(slotoffset))
		mslogger.Mslogger.Info(msg)

		if slotoffset < 96 { //offset starts from 96
			msg := fmt.Sprintf("slotoffset %d less than header size \n", slotoffset)
			mslogger.Mslogger.Info(msg)
			continue
		}

		var indexRowLen utils.SlotOffset

		if slotnum+1 < reflect.ValueOf(page.Slots).Len() { //not last one
			indexRowLen = page.Slots[slotnum+1] - slotoffset //find legnth
		} else if page.Header.FreeData < uint16(slotoffset) {
			msg := fmt.Sprintf("skipping free area starts before slot offset %d %d ", page.Header.FreeData, slotoffset)
			mslogger.Mslogger.Warning(msg)
			continue
		} else { //last slot
			indexRowLen = utils.SlotOffset(page.Header.FreeData) - slotoffset
		}

		if int(indexRowLen+slotoffset) >= len(data) {
			msg := fmt.Sprintf("exceeded buffer length %d by %d", len(data), len(data)-int(indexRowLen+slotoffset))
			mslogger.Mslogger.Warning(msg)
			break
		}
		indexRow := new(IndexRow)
		indexRow.Parse(data[slotoffset : slotoffset+indexRowLen])

		/*dst := make([]byte, page.Header.PMinLen-1)                                     // allocate memory for fixed len cols
		copy(dst, data[slotoffset+1:slotoffset+utils.SlotOffset(page.Header.PMinLen)]) //first always statusA
		indexRow.FixedLenCols = dst
		if utils.HasNullBitmap(data[slotoffset]) {
			utils.Unmarshal(data[slotoffset:slotoffset+indexRowLen], indexRow)

			indexRow.ProcessVaryingCols(data[slotoffset:], offset+int(slotoffset))

		}*/

		indexRows = append(indexRows, *indexRow)

	}
	page.IndexRows = indexRows
}

func (page *Page) parseFileHeader(data []byte) {
	//Svar fileHeader *FileHeader
	for slotnum, slotoffset := range page.Slots {
		msg := fmt.Sprintf("%d file header row at %d", slotnum, int(slotoffset))
		mslogger.Mslogger.Info(msg)

		if slotoffset < 96 { //offset starts from 96
			msg := fmt.Sprintf("slotoffset %d less than header size \n", slotoffset)
			mslogger.Mslogger.Info(msg)
			continue
		}

		var dataRow *DataRow = new(DataRow)
		var allocatedDataRowSize utils.SlotOffset

		if slotoffset == 0 {
			msg := "slotoffset is zero  potential deleted datarow \n"
			mslogger.Mslogger.Info(msg)
			continue
			//heuristics

		} else if slotoffset < 96 { //offset starts from 96
			msg := fmt.Sprintf("slotoffset %d cannot be less than 96 bytes \n", slotoffset)
			mslogger.Mslogger.Info(msg)
			continue
		}

		if page.Header.FreeData < uint16(slotoffset) {
			msg := fmt.Sprintf("skipping free area starts before slot offset %d %d ", page.Header.FreeData, slotoffset)
			mslogger.Mslogger.Warning(msg)
			continue
		}

		if slotnum+1 < reflect.ValueOf(page.Slots).Len() { //not last one
			allocatedDataRowSize = page.Slots[slotnum+1] - slotoffset //find allocated legnth
		} else { //last slot
			allocatedDataRowSize = utils.SlotOffset(page.Header.FreeData) - slotoffset
		}

		dataRow.Parse(data[slotoffset:slotoffset+allocatedDataRowSize],
			int(slotoffset), page.Header.ObjectId)

	}
}

func (page *Page) Process(data []byte, offset int, carve bool) {
	HEADERLEN := 96

	var header Header
	utils.Unmarshal(data[0:HEADERLEN], &header)

	if header.isValid() && header.sanityCheck() {
		page.Header = header
		mslogger.Mslogger.Info(fmt.Sprintf("Page Header OK Id %d Type %s Object Id %d nof slots %d",
			header.PageId, page.GetType(), page.Header.ObjectId, page.Header.SlotCnt))

		slots := retrieveSlots(data[PAGELEN-int(2*header.SlotCnt):]) //starts from end of page
		sort.Sort(utils.SortedSlotsOffset(slots))

		if len(slots) != int(header.SlotCnt) {
			mslogger.Mslogger.Info(fmt.Sprintf("Discrepancy in number of page slots declared %d actual %d", header.SlotCnt, len(slots)))
		}

		page.Slots = slots

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
		}

	}

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
