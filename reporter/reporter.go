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
	ShowComparisons     bool
	ShowDataCols        bool
	ShowPFS             bool
	ShowHeader          bool
	ShowSlots           bool
	ShowTableSchema     bool
	ShowTableContent    bool
	ShowTableAllocation string
	ShowTableIndex      bool
	ShowPageStats       bool
	ShowPageLDF         bool
	ShowRowPageInfo     bool
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
	WalkPageLSN         string
	WalkLSN             string
}

func (rp Reporter) ShowPageInfo(database db.Database, selectedPages []uint32,
	loptype string) {

	var pfsPages,
		gamPages,
		sgamPages,
		iamPages,
		diffMapPages pages.PagesPerId[uint64]

	selectedPagesMap := make(map[uint32]bool, len(selectedPages))

	for _, selectedPage := range selectedPages {
		selectedPagesMap[selectedPage] = true
	}

	if rp.ShowPageStats {
		pfsPages = database.FilterPagesByType("PFS")
		gamPages = database.FilterPagesByType("GAM")
		sgamPages = database.FilterPagesByType("SGAM")
		iamPages = database.FilterPagesByType("IAM")
		diffMapPages = database.FilterPagesByType("Differential Changed Map")
	}

	if rp.SortByLSN == "all" {
		allPages := database.PagesPerAllocUnitID.GetAllPages()
		sort.Sort(pages.SortedPagesByLSN(allPages))
		for _, page := range allPages {

			if selectedPagesMap[page.Header.PageId] || len(selectedPages) == 0 {

				rp.ShowPage(page, loptype,
					pfsPages, gamPages, sgamPages, iamPages, diffMapPages)
			}

		}
	} else {
		node := database.PagesPerAllocUnitID.GetHeadNode()
		for node != nil {
			if rp.SortByLSN == "allocunit" {
				sort.Sort(pages.SortedPagesByLSN(node.Pages))
			}
			for _, page := range node.Pages {

				if selectedPagesMap[page.Header.PageId] || len(selectedPages) == 0 {

					rp.ShowPage(page, loptype,
						pfsPages, gamPages, sgamPages, iamPages, diffMapPages)
				}
			}
			node = node.Next

		}

	}

}

func (rp Reporter) ShowComparison(baseFile, targetFile string,
	added, removed, changed []pages.PageComparison, unchanged int) {
	fmt.Printf("\nComparison: %s -> %s\n", baseFile, targetFile)
	fmt.Printf("Added pages: %d, removed pages: %d, changed pages: %d, unchanged pages: %d\n",
		len(added), len(removed), len(changed), unchanged)

	showPageChanges := func(status pages.ChangeStatus, comparisons []pages.PageComparison) {
		for _, comparison := range comparisons {
			fmt.Printf("  %s page %d allocation unit %d\n",
				status, comparison.PageID, comparison.AllocUnitID)
			for _, diff := range comparison.HeaderDiffs {
				fmt.Printf("    header.%s: %v -> %v\n", diff.Field, diff.Before, diff.After)
			}
			for _, diff := range comparison.SlotDiffs {
				if diff.After == nil {
					fmt.Printf("    slot[%d]: offset %d -> <nil>\n", diff.Index, diff.Before.Offset)
				} else if diff.Before == nil {
					fmt.Printf("    slot[%d]: offset  <nil> ->  %d\n", diff.Index, diff.After.Offset)
				} else {
					fmt.Printf("    slot[%d]: offset %d -> %d\n", diff.Index, diff.Before.Offset,
						diff.After.Offset)
				}
			}

			for _, diff := range comparison.RowDiffs {
				fmt.Printf("    row[%d].%s: %v -> %v\n", diff.Index, diff.Field, diff.Before, diff.After)
			}
			for _, diff := range comparison.MapDiffs {
				if diff.Map == "PFS" || diff.Map == "GAM" || diff.Map == "SGAM" {
					fmt.Printf("    %s page[%d].%s: %v -> %v\n", diff.Map, diff.PageID, diff.Field, diff.Before, diff.After)
					continue
				}
				fmt.Printf("    %s[%d].%s: %v -> %v\n", diff.Map, diff.Index, diff.Field, diff.Before, diff.After)
			}
		}
	}

	showPageChanges(pages.Added, added)
	showPageChanges(pages.Removed, removed)
	showPageChanges(pages.Changed, changed)
}

func (rp Reporter) ShowPage(page pages.Page, loptype string,
	pfsPages pages.PagesPerId[uint64], gamPages pages.PagesPerId[uint64],
	sgamPages pages.PagesPerId[uint64], iamPages pages.PagesPerId[uint64],
	diffMapChanges pages.PagesPerId[uint64]) {

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
		for _, record := range page.LDFRecords {
			fmt.Printf("LOP INFO \t")
			record.ShowLOPInfo(loptype)
			if rp.WalkPageLSN != "" && record.CurrentLSN.Equals(page.Header.LSN) {
				record.WalkInfo(rp.WalkPageLSN, loptype)
			}
		}
	}

	if rp.ShowPageStats {
		fmt.Printf("\n Id %d", page.Header.PageId)
		page.ShowStats(pfsPages)
		page.ShowStats(gamPages)
		page.ShowStats(sgamPages)
		page.ShowStats(iamPages)
		page.ShowStats(diffMapChanges)

	}

	if rp.ShowDBInfo && page.FileHeader != nil {
		page.FileHeader.ShowInfo()

	}

	if rp.ShowDBInfo && page.Boot != nil {
		page.Boot.ShowInfo()
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

func (rp Reporter) ShowTableInfo(wg *sync.WaitGroup, tables <-chan *db.Table) {
	defer wg.Done()

	for table := range tables {

		table.Show(rp.ShowTableSchema, rp.ShowTableContent, rp.ShowTableAllocation, rp.ShowTableIndex,
			rp.TableType, rp.ToTableRow, rp.SkippedTableRows,
			rp.SelectedTableRows, rp.ShowCarved, rp.ShowTableLDF,
			rp.ShowColNames, rp.Raw, rp.ShowRowPageInfo)

	}
}

func (rp Reporter) ShowLDFInfo(database db.Database, pagesId []uint32,
	filterlop string) {
	if database.LogDB == nil {
		fmt.Printf("No log database found for %s\n", database.Name)
		return
	}

	if rp.ShowLDF {

		database.LogDB.ShowLDF(filterlop)

		direction := rp.WalkLSN
		database.LogDB.WalkLSN(filterlop, direction)

	}

	if rp.ShowPageLDF {

		database.LogDB.ShowPagesLDF(pagesId)
	}
}
