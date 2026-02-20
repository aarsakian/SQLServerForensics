package page

import (
	"MSSQLParser/utils"
	"fmt"
	"reflect"
	"strings"
)

type GAMExtents []GAMExtent

type GAMExtent struct {
	extent    int
	allocated bool
}

func (gamExtents GAMExtents) ShowAllocations() {

	startPageId := 0

	prevGamExtent := gamExtents[0]
	fmt.Printf("GAM allocation map \n")

	for _, gamextent := range gamExtents[1:] {
		if gamextent.allocated != prevGamExtent.allocated {

			fmt.Printf("(%d:%d) = %s \n", startPageId*8, prevGamExtent.extent*8,
				(map[bool]string{true: "ALLOCATED", false: "NOT ALLOCATED"})[prevGamExtent.allocated])

			startPageId = gamextent.extent
		}

		prevGamExtent = gamextent
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

func (gamExtents GAMExtents) GetAllocationStatus(pageId []uint32) string {
	var status strings.Builder
	for _, pageId := range pageId {
		for _, gam := range gamExtents {
			if pageId < uint32(gam.extent*8) || pageId > uint32(gam.extent*8+8) {
				status.WriteString(fmt.Sprintf("%d NOT ALLOCATED\n", pageId))
			} else {
				status.WriteString(fmt.Sprintf("%d ALLOCATED\n", pageId))
			}
		}
	}
	return status.String()
}
