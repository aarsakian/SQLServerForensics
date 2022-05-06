package page

import ("fmt"
		"MSSQLParser/utils")

type PFSPage []PFS

type PFS struct {
	pageID uint8
	status string
}

func (pfsPage PFSPage) ShowAllocations(){
	for _, pfs := range pfsPage{
		fmt.Printf("page free space %d status %s \n", pfs.pageID, pfs.status)
	}
}


/*to be checked*/
func (pfsPage PFSPage) FilterByAllocationStatus(status bool) AllocationMaps {
	return PFSPage(utils.Filter(pfsPage, func(pfs PFS) bool {
		return true 
	}))

}

