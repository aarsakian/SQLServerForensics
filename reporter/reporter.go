package reporter

import (
	db "MSSQLParser/db"
	"MSSQLParser/page"
	"fmt"
)

type Reporter struct {
	ShowGamExtents      bool
	ShowSGamExtents     bool
	ShowIAMExtents      bool
	ShowDataCols        bool
	ShowPFS             bool
	ShowHeader          bool
	ShowSlots           bool
	TableName           string
	ShowTableSchema     bool
	ShowTableContent    bool
	ShowTableAllocation string
	ShowPageStats       bool
	ShowIndex           bool
	ShowTableRows       int
	ShowTableRow        int
	ShowCarved          bool
	TableType           string
}

func (rp Reporter) ShowPageInfo(database db.Database, selectedPageId uint32) {
	for _, pages := range database.PagesMap {
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

			if rp.ShowPageStats {
				if page.GetType() == "PFS" {
					pfsstatus := allocMap.GetAllocationStatus(selectedPageId)
					fmt.Printf("PFS %s ", pfsstatus)
				} else if page.GetType() == "GAM" {
					gamstatus := allocMap.GetAllocationStatus(selectedPageId)
					fmt.Printf("GAM %s ", gamstatus)
				} else if page.GetType() == "SGAM" {
					sgamstatus := allocMap.GetAllocationStatus(selectedPageId)
					fmt.Printf("SGAM %s ", sgamstatus)
				}
			}
			if rp.ShowIndex {
				page.ShowIndexRows()
			}

			if rp.ShowCarved {
				page.ShowCarvedDataRows()
			}

		}

	}

}

func (rp Reporter) ShowTableInfo(database db.Database) {
	tablename := rp.TableName
	if tablename != "" {

		database.ShowTables(tablename, rp.ShowTableSchema, rp.ShowTableContent, rp.ShowTableAllocation,
			rp.TableType, rp.ShowTableRows, rp.ShowTableRow, rp.ShowCarved)

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
