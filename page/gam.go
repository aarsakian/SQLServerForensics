package page

import ("fmt"
		"reflect"
		"MSSQLParser/utils")




type GAMExtents []GAMExtent

type GAMExtent struct {
	extent    int
	allocated bool
}

func (gamExtents GAMExtents) ShowAllocations() {
	var allocatedPages []int
	pageRange := 0
	for _, gamextent := range gamExtents {
		if gamextent.allocated {

		} else {
			allocatedPages = append(allocatedPages, pageRange)
			fmt.Printf("GAM allocated range %d \n", pageRange)

		}
		pageRange += 8
	}

}

func (gamExtents GAMExtents) FilterByAllocationStatus(status bool) AllocationMaps {
	return GAMExtents(utils.Filter(gamExtents, func(gam GAMExtent) bool {
		return gam.allocated == status
	}))

}

func (gamExtents GAMExtents) GetStats() (int, int) {
	allocatedgamextents := gamExtents.FilterByAllocationStatus(true)
	unallocatedgamextents := gamExtents.FilterByAllocationStatus(false)
	return reflect.ValueOf(allocatedgamextents).Len() * 8,
		reflect.ValueOf(unallocatedgamextents).Len() * 8

}

