package page

import (
	"MSSQLParser/utils"
	"fmt"
)

type PFSPage []PFS

type PFS struct {
	pageID uint32
	status string
}

func (pfsPage PFSPage) ShowAllocations() {
	prevStatus := ""
	startPageId := 0
	endPageId := 0
	lastPageId := 0
	for _, pfs := range pfsPage {
		if pfs.status != prevStatus {
			endPageId = int(pfs.pageID)
			fmt.Printf("(%d:%d) = %s \n", startPageId, endPageId, prevStatus)
			startPageId = int(pfs.pageID)
		}
		prevStatus = pfs.status
		lastPageId = int(pfs.pageID)
	}
	fmt.Printf("(%d:%d) = %s \n", startPageId, lastPageId, prevStatus)
}

/*to be checked*/
func (pfsPage PFSPage) FilterByAllocationStatus(status bool) AllocationMaps {
	return PFSPage(utils.Filter(pfsPage, func(pfs PFS) bool {
		return true
	}))

}
