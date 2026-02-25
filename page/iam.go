package page

//iam tracks pages that belong to an object
// the first IAM page for the object stores the actual page addresses for the first eight object pages, which are stored in mixed extents
import (
	"MSSQLParser/utils"
	"fmt"
)

const NOFSLOTS = 8

type IAM struct {
	Extents IAMExtents
	Header  *IAMHeader
}

type IAMExtents []IAMExtent

type IAMExtent struct {
	extent    int
	allocated bool
}

type IAMHeader struct {
	SequenceNumber uint64 //
	Status         uint32 //
	ObjectId       uint32
	IndexId        uint32 //
	PageCount      uint32 //

	NextPageId  utils.PageSlot //not used anymore
	PrevPageId  utils.PageSlot //not used anymore
	StartPageId utils.PageSlot //42-48
	SlotArray   []utils.PageSlot
}

func (iamHeader *IAMHeader) Parse(data []byte) {
	//start after 4 bytes
	var pageSlot utils.PageSlot
	readBytes, _ := utils.Unmarshal(data[4:], iamHeader)
	iamHeader.SlotArray = make([]utils.PageSlot, 0, 8)
	for idx := range NOFSLOTS {

		utils.Unmarshal(data[4+readBytes+idx*6:], &pageSlot)
		iamHeader.SlotArray = append(iamHeader.SlotArray, pageSlot)
	}
}

func (iamHeader IAMHeader) ShowAllocation() {
	fmt.Printf("sequencenumber =%d start_pg = (%d:%d)", iamHeader.SequenceNumber,
		iamHeader.StartPageId.FileId, iamHeader.StartPageId.PageId)
	for idx, pageSlot := range iamHeader.SlotArray {
		fmt.Printf("Slot %d = (%d:%d) \t", idx, pageSlot.FileId, pageSlot.PageId)

	}
}

func (iam IAM) FilterByAllocationStatus(status bool) AllocationMaps {
	return IAMExtents(utils.Filter(iam.Extents, func(iam IAMExtent) bool {
		return iam.allocated == status
	}))
}

func (iamExtents IAMExtents) FilterByAllocationStatus(status bool) AllocationMaps {
	return IAMExtents(utils.Filter(iamExtents, func(iam IAMExtent) bool {
		return iam.allocated == status
	}))
}

func (iam IAM) ShowAllocations() {
	iam.Header.ShowAllocation()
	iam.Extents.ShowAllocations()

}

func (iamExtents IAMExtents) ShowAllocations() {
	prevAllocatedIAM := iamExtents[0]
	startPageId := 0
	endPageId := 0
	lastPageId := 0

	fmt.Printf("\nIAM allocation map \n")
	for _, iamextent := range iamExtents[1:] {

		if iamextent.allocated != prevAllocatedIAM.allocated {
			endPageId = iamextent.extent
			fmt.Printf("(%d:%d) = %s \n", startPageId*8, endPageId*8,
				(map[bool]string{true: "NOT ALLOCATED", false: "ALLOCATED"})[prevAllocatedIAM.allocated])

			startPageId = iamextent.extent
		}
		lastPageId = iamextent.extent
		prevAllocatedIAM = iamextent
	}

	fmt.Printf("(%d:%d) = %s \n", startPageId*8, lastPageId*8,
		(map[bool]string{true: "NOT ALLOCATED", false: "ALLOCATED"})[prevAllocatedIAM.allocated])
}

func (iam IAM) GetAllocationStatus(pageId uint32) string {
	return iam.Extents.GetAllocationStatus(pageId)
}

func (iamExtents IAMExtents) GetAllocationStatus(pageId uint32) string {
	prevIAMExtent := iamExtents[0]
	for _, iam := range iamExtents[1:] {
		if pageId >= uint32(prevIAMExtent.extent)*8 && pageId < uint32(iam.extent)*8 {
			if iam.allocated {
				return " IAM ALLOCATED "
			} else {
				return " IAM NOT ALLOCATED "
			}

		}

	}

	return ""
}
