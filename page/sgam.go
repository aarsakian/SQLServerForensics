package page

import (
	"MSSQLParser/utils"
	"fmt"
	"reflect"
)

type SGAMExtents []SGAMExtent

type SGAMExtent struct {
	extent int
	mixed  bool
}

func (sgamExtents SGAMExtents) ShowAllocations() {

	startPageId := 0
	prevSgam := sgamExtents[0]

	fmt.Printf("SGAM allocation map \n")
	for _, sgamextent := range sgamExtents[1:] {
		if sgamextent.mixed != prevSgam.mixed {

			fmt.Printf("(%d:%d) = %s \n", startPageId*8, prevSgam.extent*8,
				(map[bool]string{false: "NOT ALLOCATED", true: "MIXED"})[prevSgam.mixed])

			startPageId = sgamextent.extent
		}

		prevSgam = sgamextent
	}
	fmt.Printf("(%d:%d) = %s \n", startPageId*8, prevSgam.extent*8,
		(map[bool]string{false: "NOT ALLOCATED", true: "MIXED"})[prevSgam.mixed])
}

func (sgamExtents SGAMExtents) FilterByAllocationStatus(status bool) AllocationMaps {
	return SGAMExtents(utils.Filter(sgamExtents, func(sgam SGAMExtent) bool {
		return sgam.mixed == status
	}))

}

func (sgamExtents SGAMExtents) GetAllocationStatus(pageId uint32) string {
	prevSGAM := sgamExtents[0]
	for _, sgam := range sgamExtents[1:] {
		if pageId >= uint32(prevSGAM.extent)*8 && pageId < uint32(sgam.extent)*8 {
			if sgam.mixed {
				return " SGAM MIXED "
			} else {
				return " SGAM NOT ALLOCATED "
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
