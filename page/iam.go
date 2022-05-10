package page

import (
	"MSSQLParser/utils"
	"fmt"
)

type IAMExtents []IAMExtent

type IAMExtent struct {
	extent    int
	allocated bool
}

/*type IAMHeader struct {
	sequenceNumber //position in the IAM chain
	status //
	objectId //
	indexId //
	page_count
	start_pg
	//singlePageAllocation *singlePageAllocation
}*/

func (iamExtents IAMExtents) FilterByAllocationStatus(status bool) AllocationMaps {
	return IAMExtents(utils.Filter(iamExtents, func(iam IAMExtent) bool {
		return iam.allocated == status
	}))
}

func (iamExtents IAMExtents) ShowAllocations() {
	prevAllocated := true
	startPageId := 0
	endPageId := 0
	lastPageId := 0

	fmt.Printf("IAM allocation map \n")
	for _, iamextent := range iamExtents {
		if iamextent.allocated != prevAllocated {
			endPageId = iamextent.extent
			fmt.Printf("(%d:%d) = %s \n", startPageId*8, endPageId*8,
				(map[bool]string{true: "ALLOCATED", false: "NOT ALLOCATED"})[prevAllocated])

			startPageId = iamextent.extent
		}
		lastPageId = iamextent.extent
		prevAllocated = iamextent.allocated
	}

	fmt.Printf("(%d:%d) = %s \n", startPageId*8, lastPageId*8,
		(map[bool]string{true: "ALLOCATED", false: "NOT ALLOCATED"})[prevAllocated])
}
