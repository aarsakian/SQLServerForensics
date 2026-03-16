package db

import (
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"sync"
)

type TableIndex struct {
	id          uint32
	Name        string
	rootPageId  uint32
	firstPageId uint32
	isClustered bool
	Columns     []*Column
	rows        []Row
}

func (tableIndex *TableIndex) addAllocatedPages(sysallocunit SysAllocUnits) {
	rooPageId := sysallocunit.GetRootPageId()
	if rooPageId == 0 || sysallocunit.GetDescription() != "IN_ROW_DATA" {
		return
	}

	tableIndex.firstPageId = sysallocunit.GetFirstPageId()
	tableIndex.rootPageId = rooPageId

}

func (tableIndex TableIndex) GetRecords(wg *sync.WaitGroup, selectedRows []int,
	colnames []string, records chan<- utils.Record) {
	defer wg.Done()

	for rowidx, row := range tableIndex.rows {
		var record utils.Record
		var vals []string

		for _, rownum := range selectedRows {
			if rowidx+1 == rownum {
				goto processRow
			}
		}
		if len(selectedRows) != 0 {
			continue
		}

	processRow:
		for _, c := range tableIndex.Columns {
			colData := row.ColMap[c.Name]
			vals = append(vals, c.toString(colData.Content))
		}
		record = utils.Record{Vals: vals, Carved: row.Carved, Logged: row.Logged}
		records <- record
	}
	close(records)
}

func (tindex *TableIndex) Populate(indexPages page.PagesPerId[uint32]) []uint32 {
	var rows []Row
	var pagesQueue []uint32

	var pages *page.PagesPerIdNode
	pagesQueue = append(pagesQueue, tindex.rootPageId)

	var indexedDataPages []uint32
	for len(pagesQueue) != 0 && pagesQueue[0] != 0 {

		pageId := pagesQueue[0]

		pagesQueue = pagesQueue[1:] //pop
		pages = indexPages.Lookup[pageId]
		if pages == nil {
			break
		}

		for idx := range pages.Pages[0].IndexRows {

			cmap := ColMap{}

			startOffset := 0

			VarLenIndexColOrder := 0

			for _, c := range tindex.Columns {

				/*if startOffset > len(keyValue) || startOffset+int(c.Size) > len(keyValue) {
					msg := fmt.Sprintf("data length of non-leaf index is exhausted by %d at page Id %d",
						startOffset+int(c.Size)-len(keyValue), page.Header.PageId)
					mslogger.Mslogger.Warning(msg)
					break
				}*/
				if c.isStatic() {
					if pages.Pages[0].IndexRows[idx].FixedLenCols != nil {
						cmap[c.Name] = ColData{Content: pages.Pages[0].IndexRows[idx].FixedLenCols[startOffset : startOffset+int(c.Size)]}
						startOffset += int(c.Size)
					}

				} else {
					if pages.Pages[0].IndexRows[idx].VarLenCols != nil {
						//not all var len cols are part of the index key, only those defined as index key columns are, so we need to check the
						// var len order of the column to know which varying length column in the index row corresponds
						// to the column in the table schema
						cmap[c.Name] = ColData{Content: (*pages.Pages[0].IndexRows[idx].VarLenCols)[VarLenIndexColOrder].Content}
						VarLenIndexColOrder++
					}

				}

			}
			//remaining is indexnonleaf data
			if startOffset < len(pages.Pages[0].IndexRows[idx].FixedLenCols) {
				indexNonLeaf := new(page.IndexNoNLeaf)
				utils.Unmarshal(pages.Pages[0].IndexRows[idx].FixedLenCols[startOffset:],
					indexNonLeaf)

				//top level index pages point to other index pages until the leaf level is reached, at which point the index rows point to data pages
				if pages.Pages[0].Header.Level > 1 {
					pagesQueue = append(pagesQueue, indexNonLeaf.ChildPageID)
				} else {
					indexedDataPages = append(indexedDataPages, indexNonLeaf.ChildPageID)
				}
				pages.Pages[0].IndexRows[idx].NoNLeaf = indexNonLeaf
			}

			rows = append(rows, Row{ColMap: cmap})
		}

	}
	tindex.rows = rows

	return indexedDataPages
	/*sort indexes
	slices.SortFunc(rows, func(first, second Row) int {
		var res int
		for cname, fcol := range first.ColMap {
			res = slices.CompareFunc(fcol.Content, second.ColMap[cname].Content,
				func(fbyte byte, sbyte byte) int {
					return cmp.Compare(fbyte, sbyte)
				})
			if res == 0 {
				continue
			}
			return res
		}
		return res

	})
	*/

}
