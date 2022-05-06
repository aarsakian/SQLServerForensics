package page

import ("fmt"
		"reflect"
		"MSSQLParser/utils")



type SGAMExtents []SGAMExtent

type SGAMExtent struct {
	extent    int
	allocated bool
}

func (sgamExtents SGAMExtents) ShowAllocations() {
	var allocatedPages []int
	pageRange := 0
	for _, sgamextent := range sgamExtents {
		if sgamextent.allocated {

		} else {
			allocatedPages = append(allocatedPages, pageRange)
			fmt.Printf("SGAM allocated range %d \n", pageRange)
		}
		pageRange += 8
	}
}

func (sgamExtents SGAMExtents) FilterByAllocationStatus(status bool) AllocationMaps {
	return SGAMExtents(utils.Filter(sgamExtents, func(sgam SGAMExtent) bool {
		return sgam.allocated == status
	}))

}

func (sgamExtents SGAMExtents) GetStats() (int, int) {
	allocatedgamextents := sgamExtents.FilterByAllocationStatus(true)
	unallocatedgamextents := sgamExtents.FilterByAllocationStatus(false)
	return reflect.ValueOf(allocatedgamextents).Len() * 8,
		reflect.ValueOf(unallocatedgamextents).Len() * 8

}
