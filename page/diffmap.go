package page

import (
	"MSSQLParser/utils"
	"fmt"
)

type DiffMapExtents []DiffMap

type DiffMap struct {
	extent  int
	changed bool
}

func (diffmapextents DiffMapExtents) ShowAllocations() {

	startPageId := 0

	prevDiffMapExtent := diffmapextents[0]
	fmt.Printf("Diff Map allocation map \n")

	for _, diffmapextent := range diffmapextents[1:] {
		if diffmapextent.changed != prevDiffMapExtent.changed {

			fmt.Printf("(%d:%d) = %s \n", startPageId*8, prevDiffMapExtent.extent*8,
				(map[bool]string{true: "NOT CHANGED", false: "CHANGED"})[prevDiffMapExtent.changed])

			startPageId = prevDiffMapExtent.extent + 1
		}

		prevDiffMapExtent = diffmapextent
	}

	fmt.Printf("(%d:%d) = %s \n", startPageId*8, prevDiffMapExtent.extent*8,
		(map[bool]string{true: "NOT CHANGED", false: "CHANGED"})[prevDiffMapExtent.changed])

}

func (diffmapextents DiffMapExtents) FilterByAllocationStatus(changed bool) AllocationMaps {
	return DiffMapExtents(utils.Filter(diffmapextents, func(diffmap DiffMap) bool {
		return diffmap.changed == changed
	}))

}

func (diffmapExtents DiffMapExtents) GetAllocationStatus(pageId uint32) string {
	prevDiffMap := diffmapExtents[0]
	for _, diffmap := range diffmapExtents[1:] {
		if pageId >= uint32(prevDiffMap.extent)*8 && pageId < uint32(diffmap.extent)*8 {
			return " NOT CHANGED "
		} else {
			return " CHANGED "
		}
	}

	return ""
}
