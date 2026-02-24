package reporter

import (
	db "MSSQLParser/db"
	pages "MSSQLParser/page"
	"fmt"
	"sort"
	"sync"
)

type Reporter struct {
	ShowDBInfo          bool
	ShowGamExtents      bool
	ShowSGamExtents     bool
	ShowIAMExtents      bool
	ShowBCMExtents      bool
	ShowDiffMapExtents  bool
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
	SortByLSN           string
	WalkLSN             string
}

func (rp Reporter) ShowPageInfo(database db.Database, selectedPages []uint32,
	loptype string) {

	if rp.ShowPageStats {

		database.ShowStats()
		/*gamstatus := allocMap.GetAllocationStatus(selectedPages)
		fmt.Printf("GAM %s ", gamstatus)

		sgamstatus := allocMap.GetAllocationStatus(selectedPages)
		fmt.Printf("SGAM %s ", sgamstatus)*/

	}

	if rp.SortByLSN == "all" {
		allPages := database.PagesPerAllocUnitID.GetAllPages()
		sort.Sort(pages.SortedPagesByLSN(allPages))
		for _, page := range allPages {
			for _, selectedPage := range selectedPages {
				if page.Header.PageId == selectedPage {
					rp.ShowPage(page, loptype)
				}
			}
			if len(selectedPages) == 0 {
				rp.ShowPage(page, loptype)
			}

		}
	} else {
		node := database.PagesPerAllocUnitID.GetHeadNode()
		for node != nil {
			if rp.SortByLSN == "allocunit" {
				sort.Sort(pages.SortedPagesByLSN(node.Pages))
			}
			for _, page := range node.Pages {
				for _, selectedPage := range selectedPages {
					if page.Header.PageId == selectedPage {
						rp.ShowPage(page, loptype)
					}
				}
				if len(selectedPages) == 0 {
					rp.ShowPage(page, loptype)
				}
			}
			node = node.Next

		}

	}

}

func (rp Reporter) ShowPage(page pages.Page, loptype string) {

	if rp.ShowPFS && page.GetType() == "PFS" ||
		rp.ShowIAMExtents && page.GetType() == "IAM" ||
		rp.ShowGamExtents && page.GetType() == "GAM" ||
		rp.ShowSGamExtents && page.GetType() == "SGAM" ||
		rp.ShowDiffMapExtents && page.GetType() == "Differential Changed Map" ||
		rp.ShowBCMExtents && page.GetType() == "Bulk Change Map" {
		allocMap := page.GetAllocationMaps()
		allocMap.ShowAllocations()
	}
	if rp.ShowHeader {
		page.PrintHeader(rp.ShowSlots)
		if page.LDFRecord != nil {
			fmt.Printf("LOP INFO \t")
			page.LDFRecord.ShowLOPInfo(loptype)
			if rp.WalkLSN != "" {
				page.LDFRecord.WalkInfo(rp.WalkLSN, loptype)
			}
		}
	}

	if rp.ShowDBInfo && page.FileHeader != nil {
		page.FileHeader.ShowInfo()
	}

	if rp.ShowDataCols {
		page.ShowRowData()
	}

	if rp.ShowSlots {
		page.ShowSlotInfo()
	}

	if rp.ShowIndex {
		page.ShowIndexRows()
	}

	if rp.ShowCarved && rp.ShowDataCols {
		page.ShowCarvedDataRows()
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
