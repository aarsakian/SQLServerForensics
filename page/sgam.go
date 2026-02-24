package page

import (
	"MSSQLParser/utils"
	"fmt"
	"reflect"
)

type SGAMExtents []SGAMExtent

type SGAMExtent struct {
	pageid int
	mixed  bool
}

func (sgamExtents SGAMExtents) ShowAllocations() {

	startPageId := 0
	prevSgamAllocated := sgamExtents[0]

	fmt.Printf("SGAM allocation map \n")
	for _, sgamextent := range sgamExtents[1:] {
		if sgamextent.mixed != prevSgamAllocated.mixed {

			fmt.Printf("(%d:%d) = %s \n", startPageId*8, prevSgamAllocated.pageid*8,
				(map[bool]string{false: "ALLOCATED", true: "NOT ALLOCATED"})[prevSgamAllocated.mixed])

			startPageId = sgamextent.pageid
		}

		prevSgamAllocated = sgamextent
	}

	fmt.Printf("(%d:%d) = %s \n", startPageId*8, prevSgamAllocated.pageid*8,
		(map[bool]string{true: "ALLOCATED", false: "NOT ALLOCATED"})[prevSgamAllocated.mixed])
}

func (sgamExtents SGAMExtents) FilterByAllocationStatus(status bool) AllocationMaps {
	return SGAMExtents(utils.Filter(sgamExtents, func(sgam SGAMExtent) bool {
		return sgam.mixed == status
	}))

}

func (sgamExtents SGAMExtents) GetAllocationStatus(pageId uint32) string {

	for _, sgam := range sgamExtents {
		if pageId == uint32(sgam.pageid*8) {
			if sgam.mixed {
				return " SGAM Mixed "
			} else {
				return " SGAM Not Mixed "
			}

		}
	}

	return ""
}

func (sgamExtents SGAMExtents) GetStats() (int, int) {
	allocatedgamextents := sgamExtents.FilterByAllocationStatus(true)
	unallocatedgamextents := sgamExtents.FilterByAllocationStatus(false)
	return reflect.ValueOf(allocatedgamextents).Len() * 8,
		reflect.ValueOf(unallocatedgamextents).Len() * 8

}
