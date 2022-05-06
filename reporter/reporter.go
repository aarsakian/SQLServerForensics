package reporter

import ("fmt"
		db "MSSQLParser/db"
		"MSSQLParser/page")



type Reporter struct{
	ShowGamExtents bool
	ShowSGamExtents bool
	ShowIAMExtents bool
}

func (rp Reporter) ShowStats(database db.Database) {
	for _, page := range database.Pages {
		allocMap := page.GetAllocationMaps()
		if allocMap == nil {
			continue
		}
		allocMap.ShowAllocations()
	}

}

func (rp Reporter) PrintHeaderInfo(page page.Page){

	
}
