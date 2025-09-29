package page

import (
	"MSSQLParser/utils"
	"fmt"
	"reflect"
	"strings"
)

type SGAMExtents []SGAMExtent

type SGAMExtent struct {
	extent    int
	allocated bool
}

func (sgamExtents SGAMExtents) ShowAllocations() {

	prevAllocated := true
	startPageId := 0
	endPageId := 0
	lastPageId := 0

	fmt.Printf("SGAM allocation map \n")
	for _, sgamextent := range sgamExtents {
		if sgamextent.allocated != prevAllocated {
			endPageId = sgamextent.extent
			fmt.Printf("(%d:%d) = %s \n", startPageId*8, endPageId*8,
				(map[bool]string{true: "ALLOCATED", false: "NOT ALLOCATED"})[prevAllocated])

			startPageId = sgamextent.extent
		}
		lastPageId = sgamextent.extent
		prevAllocated = sgamextent.allocated
	}

	fmt.Printf("(%d:%d) = %s \n", startPageId*8, lastPageId*8,
		(map[bool]string{true: "ALLOCATED", false: "NOT ALLOCATED"})[prevAllocated])
}

func (sgamExtents SGAMExtents) FilterByAllocationStatus(status bool) AllocationMaps {
	return SGAMExtents(utils.Filter(sgamExtents, func(sgam SGAMExtent) bool {
		return sgam.allocated == status
	}))

}

func (sgamExtents SGAMExtents) GetAllocationStatus(pagesId []uint32) string {
	var status strings.Builder
	status.WriteString("NOT ALLOCATED\n")

	for _, pageId := range pagesId {
		for _, sgam := range sgamExtents {
			if pageId < uint32(sgam.extent*8) || pageId > uint32(sgam.extent*8+8) {
				continue
			}
			status.WriteString(fmt.Sprintf("%d ALLOCATED\n", pageId))
		}
	}

	return status.String()
}

func (sgamExtents SGAMExtents) GetStats() (int, int) {
	allocatedgamextents := sgamExtents.FilterByAllocationStatus(true)
	unallocatedgamextents := sgamExtents.FilterByAllocationStatus(false)
	return reflect.ValueOf(allocatedgamextents).Len() * 8,
		reflect.ValueOf(unallocatedgamextents).Len() * 8

}
