package page

import (
	"MSSQLParser/utils"
	"fmt"
	"sort"
	"strings"
)

//mixed extent sharing data more than one table
//every byte stores information about a specific page
//PFS pages, which track the allocation status of individual pages within a 64
//MB section of a data file (a PFS interval
/*00010000 =
00100000 =
*/
const (
	PFSFree_100 = 0x00

	PFSFree_50_100_ = 0x01

	PFSFree_20_50_ = 0x02

	PFSFree_5_20_ = 0x03
	PFSFree_0_5   = 0x04
)

const (
	Allocated          = 0x40
	MixedExtent        = 0x20
	IndexAllocationMap = 0x10
	Ghost              = 0x08
)

var PFSStatus = map[uint8]string{

	Ghost:              "Ghost records present ",
	IndexAllocationMap: "Iam page ", MixedExtent: "Mixed extent "}

type PFSPage []PFS

type PFS struct {
	pageID uint32
	status uint8
}

func (pfsPage PFSPage) ShowAllocations() {
	prevStatus := ""
	startPageId := pfsPage[0].pageID
	prevRawStatus := uint8(0)
	lastPageId := 0
	for idx, pfs := range pfsPage {

		if idx > 0 && pfs.status != prevRawStatus {

			fmt.Printf("Page Id: %d - %d = %s  %d \n", startPageId, int(pfs.pageID)-2, prevStatus, prevRawStatus)
			startPageId = pfs.pageID - 1
		}

		prevStatus = pfs.decodeStatus()
		prevRawStatus = pfs.status
		lastPageId = int(pfs.pageID) - 1
	}
	fmt.Printf("Page ID: %d - %d  = %s \n", startPageId, lastPageId, prevStatus)
}

/*to be checked*/
func (pfsPage PFSPage) FilterByAllocationStatus(status bool) AllocationMaps {
	return PFSPage(utils.Filter(pfsPage, func(pfs PFS) bool {
		return true
	}))

}

func (pfsPage PFSPage) GetAllocationStatus(pageId uint32) string {
	var status strings.Builder

	for _, pfs := range pfsPage {
		if pfs.pageID != pageId {
			continue
		}
		fmt.Fprintf(&status, "%s ", pfs.decodeStatus())
	}

	return status.String()
}

func (pfs PFS) decodeStatus() string {
	var status strings.Builder

	//upper 3 bits

	if pfs.status&Allocated == Allocated {
		status.WriteString("ALLOCATED ")
	} else {
		status.WriteString("NOT ALLOCATED ")
	}

	switch pfs.status & 0x07 {
	case PFSFree_0_5:
		status.WriteString("100_PCT_FULL ")
	case PFSFree_50_100_:
		status.WriteString("50_PCT_FULL ")
	case PFSFree_20_50_:
		status.WriteString("80_PCT_FULL ")
	case PFSFree_5_20_:
		status.WriteString("95 PCT_FULL ")
	case PFSFree_100:

		status.WriteString("0_PCT_FULL ")

	}

	keys := make([]int, 0, len(PFSStatus))
	for k := range PFSStatus {
		keys = append(keys, int(k))
	}

	sort.Ints(keys)

	for _, k := range keys {
		if pfs.status&uint8(k) == uint8(k) {

			status.WriteString(PFSStatus[uint8(k)])
		}
	}

	return status.String()
}
