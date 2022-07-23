package reporter

import (
	db "MSSQLParser/db"
	"MSSQLParser/page"
	"fmt"
)

type Reporter struct {
	ShowGamExtents  bool
	ShowSGamExtents bool
	ShowIAMExtents  bool
	ShowDataCols    bool
	ShowPFS         bool
	ShowHeader      bool
	ShowSlots       bool
	TableName       string
}

func (rp Reporter) ShowStats(database db.Database) {
	for _, pages := range database.Pages {
		for _, page := range pages {
			allocMap := page.GetAllocationMaps()

			if rp.ShowPFS && page.GetType() == "PFS" ||
				rp.ShowIAMExtents && page.GetType() == "IAM" ||
				rp.ShowGamExtents && page.GetType() == "GAM" ||
				rp.ShowSGamExtents && page.GetType() == "SGAM" {
				allocMap.ShowAllocations()
			}
			if rp.ShowHeader {
				page.PrintHeader(rp.ShowSlots)
			}

			if rp.ShowDataCols {
				page.ShowRowData()
			}

		}

	}
	if rp.TableName != "" {
		tablename := rp.TableName
		database.ShowTables(tablename)
	}

}

func (rp Reporter) PrintDataRowInfo(page page.Page) {
	for slotId, dataRow := range page.DataRows {
		fmt.Printf("Slot %d Record size offset %x \n", slotId, page.Slots[slotId])
		if rp.ShowDataCols {
			dataRow.ShowData()
		}

	}
}
