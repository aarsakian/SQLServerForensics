package page

import (
	"MSSQLParser/utils"
	"fmt"
	"reflect"
)

type GAMExtents []GAMExtent

type GAMExtent struct {
	extent    int
	allocated bool
}

func (gam GAMExtent) GetAllocationStatus() string {
	if gam.allocated {
		return "ALLOCATED"
	}
	return "NOT ALLOCATED"
}

func compareGAM(comparison *PageComparison, base, target *GAMExtents) {
	for index := 0; index < utils.LengthOf(base) || index < utils.LengthOf(target); index++ {
		if base != nil && target != nil && index < len(*base) && index < len(*target) {
			addMapFieldAtPage(comparison, "GAM", index, uint32((*target)[index].extent*8), "allocation", (*base)[index].GetAllocationStatus(), (*target)[index].GetAllocationStatus())
			continue
		}
		var before, after any
		if base != nil && index < len(*base) {
			before = (*base)[index].GetAllocationStatus()
		}
		if target != nil && index < len(*target) {
			after = (*target)[index].GetAllocationStatus()
		}
		pageID := uint32(0)
		if target != nil && index < len(*target) {
			pageID = uint32((*target)[index].extent * 8)
		} else if base != nil && index < len(*base) {
			pageID = uint32((*base)[index].extent * 8)
		}
		addMapFieldAtPage(comparison, "GAM", index, pageID, "allocation", before, after)
	}
}

func (gamExtents GAMExtents) ShowAllocations() {

	startextent := 0

	prevGamExtent := gamExtents[0]
	fmt.Printf("GAM allocation map \n")

	for _, gamextent := range gamExtents[1:] {
		if gamextent.allocated != prevGamExtent.allocated {

			fmt.Printf("(%d:%d) = %s \n", startextent*8, prevGamExtent.extent*8,
				(map[bool]string{true: "ALLOCATED", false: "NOT ALLOCATED"})[prevGamExtent.allocated])

			startextent = gamextent.extent
		}

		prevGamExtent = gamextent
	}
	fmt.Printf("(%d:%d) = %s \n", startextent*8, prevGamExtent.extent*8,
		(map[bool]string{true: "ALLOCATED", false: "NOT ALLOCATED"})[prevGamExtent.allocated])

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

func (gamExtents GAMExtents) GetAllocationStatus(pageId uint32) string {
	prevGAM := gamExtents[0]
	for _, gam := range gamExtents[1:] {
		if pageId >= uint32(prevGAM.extent)*8 && pageId < uint32(gam.extent)*8 {
			if gam.allocated { //
				return " GAM ALLOCATED "
			} else {
				return " GAM ΝΟΤ ALLOCATED "
			}

		}
	}

	return ""
}
