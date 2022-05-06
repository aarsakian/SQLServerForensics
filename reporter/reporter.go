package reporter

import ("fmt"
		db "MSSQLParser/db"
		"MSSQLParser/page"
		)



type Reporter struct{
	ShowGamExtents bool
	ShowSGamExtents bool
	ShowIAMExtents bool
	ShowDataCols bool
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
	header := page.Header
	fmt.Printf("Type %s Object is %d slots %d free space %d\n", page.GetType(), 
	header.ObjectId, header.SlotCnt, header.FreeData)
}



func (rp Reporter) PrintDataRowInfo(page page.Page){
	for slotId, dataRow := range page.DataRows {
		fmt.Printf("Slot %d Record size offset %x \n", slotId, page.Slots[slotId])
		if rp.ShowDataCols {
			dataRow.ShowData()
		}
		
	
		

	}
}


