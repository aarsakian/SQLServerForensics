package reporter

import (
	db "MSSQLParser/db"
	"MSSQLParser/page"
	"fmt"
	"sync"
)

type Reporter struct {
	ShowGamExtents      bool
	ShowSGamExtents     bool
	ShowIAMExtents      bool
	ShowDataCols        bool
	ShowPFS             bool
	ShowHeader          bool
	ShowSlots           bool
	ShowTableSchema     bool
	ShowTableContent    bool
	ShowTableAllocation string
	ShowTableIndex      bool
	ShowPageStats       bool
	ShowIndex           bool
	ToTableRow          int
	SkippedTableRows    int
	SelectedTableRows   []int
	ShowCarved          bool
	ShowLDF             bool
	TableType           string
	Raw                 bool
	ShowColNames        []string
}

func (rp Reporter) ShowPageInfo(database db.Database, selectedPageId uint32) {
	node := database.PagesPerAllocUnitID.GetHeadNode()
	for node != nil {

		for _, page := range node.Pages {
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

			if rp.ShowCarved && rp.ShowDataCols {
				page.ShowCarvedDataRows()
			}

		}
		node = node.Next

	}

}

func (rp Reporter) ShowTableInfo(wg *sync.WaitGroup, tables <-chan db.Table) {
	defer wg.Done()

	for table := range tables {

		table.Show(rp.ShowTableSchema, rp.ShowTableContent, rp.ShowTableAllocation, rp.ShowTableIndex,
			rp.TableType, rp.ToTableRow, rp.SkippedTableRows,
			rp.SelectedTableRows, rp.ShowCarved, rp.ShowLDF, rp.ShowColNames, rp.Raw)

	}
}

func (rp Reporter) ShowLDFInfo(database db.Database, filterlop string) {
	if rp.ShowLDF {
		database.ShowLDF(filterlop)

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
