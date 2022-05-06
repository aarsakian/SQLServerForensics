package page

import ("fmt"
		"MSSQLParser/utils")


type IAMExtents []IAMExtent

type IAMExtent struct {
	extent int 
	allocated bool 
}

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