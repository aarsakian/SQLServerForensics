package page

import (
	"MSSQLParser/utils"
	"fmt"
	"strings"
)

//mixed extent sharing data more than one table
//every byte stores information about a specific page

var PFSStatus = map[uint8]string{
	0: "NOT ALLOCATED 0PCT_FULL", 8: "NOT ALLOCATED 100PCT_FULL", 68: "ALLOCATED 100FULL",
	96: "ALLOCATED Mixed Extent 0PTC_FULL", 116: "ALLOCATED Mixed Extent IAM 100PCT_FULL",
	112: "ALLOCATED Mixed Extent IAM EMPTY", 64: "ALLOCATED EMPTY", 65: "ALLOCATED 50PCT_FULL",
	66: "ALLOCATED 80PCT_FULL", 67: "ALLOCATED 95PCT_FULL", 156: "UNUSED HAS_GHOST D 100PCT_FULL"}

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

func (pfsPage PFSPage) GetAllocationStatus(pagesId []uint32) string {
	var status strings.Builder
	for _, pageId := range pagesId {
		for _, pfs := range pfsPage {
			if pfs.pageID != pageId {
				continue
			}
			status.WriteString(fmt.Sprintf("%d %s\n", pfs.pageID, pfs.status))
		}
	}
	return status.String()
}
