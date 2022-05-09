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
	var allocatedPages []int
	pageRange := 0
	for _, iamextent := range iamExtents {
		if iamextent.allocated {

		} else {
			allocatedPages = append(allocatedPages, pageRange)
			fmt.Printf("IAM allocated range %d \n", pageRange)
		}
		pageRange += 8
	}
}
