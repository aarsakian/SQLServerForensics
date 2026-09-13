package page

import (
	"MSSQLParser/utils"
	"fmt"
	"strings"
)

type BulkChangeMapExtents []BulkChangeMap
type BulkChangeMap struct {
	extent  int
	changed bool
}

func compareBulkChangeMap(comparison *PageComparison, base, target *BulkChangeMapExtents) {
	for index := 0; index < utils.LengthOf(base) || index < utils.LengthOf(target); index++ {
		if base != nil && target != nil && index < len(*base) && index < len(*target) {
			addMapField(comparison, "BulkChangeMap", index, "extent", (*base)[index].extent, (*target)[index].extent)
			addMapField(comparison, "BulkChangeMap", index, "changed", (*base)[index].changed, (*target)[index].changed)
			continue
		}
		addMapEntryDiff(comparison, "BulkChangeMap", index, base, target, index)
	}
}

func (bcmextents BulkChangeMapExtents) ShowAllocations() {

	startPageId := 0

	prevDiffMapExtent := bcmextents[0]
	fmt.Printf("Bulk Difference Map allocation map \n")

	for _, bcmextent := range bcmextents[1:] {
		if bcmextent.changed != prevDiffMapExtent.changed {

			fmt.Printf("(%d:%d) = %s \n", startPageId*8, prevDiffMapExtent.extent*8,
				(map[bool]string{true: "NOT MIN LOGGED", false: "LOGGED"})[prevDiffMapExtent.changed])

			startPageId = prevDiffMapExtent.extent
		}

		prevDiffMapExtent = bcmextent
	}
	fmt.Printf("(%d:%d) = %s \n", startPageId*8, prevDiffMapExtent.extent*8,
		(map[bool]string{true: "NOT MIN LOGGED", false: "LOGGED"})[prevDiffMapExtent.changed])
}

func (bcmextents BulkChangeMapExtents) FilterByAllocationStatus(changed bool) AllocationMaps {
	return BulkChangeMapExtents(utils.Filter(bcmextents, func(bcm BulkChangeMap) bool {
		return bcm.changed == changed
	}))

}

func (bcmextents BulkChangeMapExtents) GetAllocationStatus(pageId uint32) string {
	var status strings.Builder

	for _, bcm := range bcmextents {
		if pageId < uint32(bcm.extent*8) || pageId > uint32(bcm.extent*8+8) {
			status.WriteString(fmt.Sprintf("%d NOT MIN LOGGED\n", pageId))
		} else {
			status.WriteString(fmt.Sprintf("%d MIN LOGGED \n", pageId))
		}
	}

	return status.String()
}
