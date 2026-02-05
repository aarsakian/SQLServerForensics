package reporter

import (
	db "MSSQLParser/db"
	pages "MSSQLParser/page"
	"fmt"
	"sort"
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
	ShowTableLDF        bool
	ShowLDF             bool
	TableType           string
	Raw                 bool
	ShowColNames        []string
	SortByLSN           bool
}

func (rp Reporter) ShowPageInfo(database db.Database, selectedPages []uint32) {
	node := database.PagesPerAllocUnitID.GetHeadNode()
	for node != nil {
		if rp.SortByLSN {
			sort.Sort(pages.SortedPagesByLSN(node.Pages))
		}
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

			if rp.ShowSlots {
				page.ShowSlotInfo()
			}

			if rp.ShowPageStats {
				if page.GetType() == "PFS" {
					pfsstatus := allocMap.GetAllocationStatus(selectedPages)
					fmt.Printf("PFS %s ", pfsstatus)
				} else if page.GetType() == "GAM" {
					gamstatus := allocMap.GetAllocationStatus(selectedPages)
					fmt.Printf("GAM %s ", gamstatus)
				} else if page.GetType() == "SGAM" {
					sgamstatus := allocMap.GetAllocationStatus(selectedPages)
					fmt.Printf("SGAM %s ", sgamstatus)
				} else {
					fmt.Printf("PFS, GAM, SGAM, DATA page type not found")
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
		fmt.Printf("Table %s\n", table.Name)
		table.Show(rp.ShowTableSchema, rp.ShowTableContent, rp.ShowTableAllocation, rp.ShowTableIndex,
			rp.TableType, rp.ToTableRow, rp.SkippedTableRows,
			rp.SelectedTableRows, rp.ShowCarved, rp.ShowTableLDF,
			rp.ShowColNames, rp.Raw)

	}
}

func (rp Reporter) ShowLDFInfo(database db.Database, pagesId []uint32,
	filterlop string) {
	if rp.ShowLDF {
		database.ShowLDF(filterlop)
		database.ShowPagesLDF(pagesId)

	}
}
