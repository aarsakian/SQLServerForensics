package manager

import (
	"MSSQLParser/db"
	"MSSQLParser/page"
)

type DatabaseComparison struct {
	BaseFile   string
	TargetFile string
	Added      []page.PageComparison
	Removed    []page.PageComparison
	Changed    []page.PageComparison
	Unchanged  int
}

type databasePageKey struct {
	AllocUnitID uint64
	PageID      uint32
}

func CompareDatabases(base, target db.Database) DatabaseComparison {
	comparison := DatabaseComparison{BaseFile: base.Fname, TargetFile: target.Fname}
	basePages := collectPages(base)
	targetPages := collectPages(target)

	for key, targetPage := range targetPages {
		basePage, ok := basePages[key]
		if !ok {
			comparison.Added = append(comparison.Added, page.PageComparison{
				PageID: targetPage.Header.PageId, AllocUnitID: key.AllocUnitID,
				Status: page.Added,
			})
			continue
		}

		pageComparison := page.ComparePages(basePage, targetPage)
		if pageComparison.Status == page.Unchanged {
			comparison.Unchanged++
		} else {
			comparison.Changed = append(comparison.Changed, pageComparison)
		}
	}

	for key, basePage := range basePages {
		if _, ok := targetPages[key]; ok {
			continue
		}
		comparison.Removed = append(comparison.Removed, page.PageComparison{
			PageID: basePage.Header.PageId, AllocUnitID: key.AllocUnitID,
			Status: page.Removed,
		})
	}

	return comparison
}

func collectPages(database db.Database) map[databasePageKey]page.Page {
	pages := make(map[databasePageKey]page.Page)
	if database.PagesPerAllocUnitID.IsEmpty() {
		return pages
	}
	node := database.PagesPerAllocUnitID.GetHeadNode()
	for node != nil {
		for _, currentPage := range node.Pages {
			key := databasePageKey{
				AllocUnitID: currentPage.Header.GetMetadataAllocUnitId(),
				PageID:      currentPage.Header.PageId,
			}
			pages[key] = currentPage
		}
		node = node.Next
	}
	return pages
}

func (pm *ProcessManager) CompareWithBase(targetName string) (DatabaseComparison, bool) {
	if pm.BaseDatabase == nil {
		return DatabaseComparison{}, false
	}
	target, ok := pm.Databases[targetName]
	if !ok {
		return DatabaseComparison{}, false
	}
	comparison := CompareDatabases(*pm.BaseDatabase, target)
	if pm.Comparisons == nil {
		pm.Comparisons = make(map[string]DatabaseComparison)
	}
	pm.Comparisons[targetName] = comparison
	return comparison, true
}

func (pm ProcessManager) ShowComparisons() {
	if !pm.reporter.ShowComparisons {
		return
	}
	for _, comparison := range pm.Comparisons {
		pm.reporter.ShowComparison(comparison.BaseFile, comparison.TargetFile,
			comparison.Added, comparison.Removed, comparison.Changed, comparison.Unchanged)
	}
}

func (pm *ProcessManager) ProcessBaseMDF(filename string, selectedPages []int, fromPage int, toPage int, carve bool) (int, error) {
	database := db.Database{Fname: filename}
	processedPages, err := database.ProcessMDF(selectedPages, fromPage, toPage, carve)
	if err != nil || processedPages == 0 {
		return processedPages, err
	}

	database.ProcessSystemTables()
	database.LinkPartitionIdToObjectId()
	pm.BaseDatabase = &database
	return processedPages, nil
}
