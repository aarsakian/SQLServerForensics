package manager

import (
	"MSSQLParser/db"
	"MSSQLParser/exporter"
	"MSSQLParser/reporter"
	"fmt"
	"sync"
)

type ProcessManager struct {
	reporter  reporter.Reporter
	exporter  exporter.Exporter
	databases []db.Database
}

func (PM *ProcessManager) Initialize(showGamExtents bool, showSGamExtents bool, showIAMExtents bool,
	showDataCols bool, showPFS bool, showHeader bool, showSlots bool, showTableSchema bool,
	showTableContent bool, showTableAllocation string,
	showTableIndex bool, showPageStats bool, showIndex bool, toTableRow int,
	skippedTableRows int, selectedTableRows []int, showcarved bool,
	showLDF bool, tabletype string, raw bool, colnames []string,
	exportFormat string, exportImage bool, exportPath string) {

	PM.reporter = reporter.Reporter{ShowGamExtents: showGamExtents,
		ShowSGamExtents:     showSGamExtents,
		ShowIAMExtents:      showIAMExtents,
		ShowDataCols:        showDataCols,
		ShowPFS:             showPFS,
		ShowHeader:          showHeader,
		ShowSlots:           showSlots,
		ShowTableSchema:     showTableSchema,
		ShowTableContent:    showTableContent,
		ShowTableAllocation: showTableAllocation,
		ShowTableIndex:      showTableIndex,
		ShowPageStats:       showPageStats,
		ShowIndex:           showIndex,
		ToTableRow:          toTableRow,
		SkippedTableRows:    skippedTableRows,
		SelectedTableRows:   selectedTableRows,
		ShowCarved:          showcarved,
		ShowLDF:             showLDF,
		TableType:           tabletype,
		Raw:                 raw,
		ShowColNames:        colnames}

	PM.exporter = exporter.Exporter{Format: exportFormat, Image: exportImage, Path: exportPath}
}

func (PM *ProcessManager) ProcessDBFiles(mdffiles []string, ldffiles []string,
	selectedPage int, fromPage int, toPage int, ldf bool, showcarved bool) int {

	var database db.Database

	processedPages := 0
	for idx, inputFile := range mdffiles {
		if len(ldffiles) > 0 {
			database = db.Database{Fname: inputFile, Lname: ldffiles[idx]}
		} else {
			database = db.Database{Fname: inputFile}
		}

		/*processing pages stage */
		totalProcessedPages, err := database.ProcessMDF(selectedPage, fromPage, toPage, showcarved)
		if err != nil {
			continue
		}

		if totalProcessedPages == 0 {
			fmt.Printf("no pages found skipped processing\n")
			continue
		}

		database.ProcessSystemTables()

		if ldf {
			ldfRecordsProcessed, err := database.ProcessLDF()

			if err == nil && ldfRecordsProcessed > 0 {
				database.LocateLogRecords()
			}

		}

		processedPages += totalProcessedPages
		PM.databases = append(PM.databases, database)
	}
	return processedPages

}

func (PM *ProcessManager) FilterDatabases(pageType string, systemTables string, userTable string) {
	for dbidx := range PM.databases {
		if pageType != "" {
			PM.databases[dbidx].FilterPagesByType(pageType) //mutable

		}

		if systemTables != "" {
			PM.databases[dbidx].FilterPagesBySystemTables(systemTables)

		}

		if userTable != "" {
			PM.databases[dbidx].FilterPagesBySystemTables("sysschobjs")
		}
	}

}

func (PM ProcessManager) ProcessDBTables(wg *sync.WaitGroup,
	tablenames []string, tabletype string, tablepages []int,
	colnames []string, represults map[string]chan db.Table,
	expresults map[string]chan db.Table) {

	for _, database := range PM.databases {
		represults[database.Name] = make(chan db.Table, 10000)
		expresults[database.Name] = make(chan db.Table, 10000)
		/*retrieving schema and table contents */

		go database.ProcessTables(wg, tablenames, tabletype,
			represults[database.Name], expresults[database.Name], tablepages)

	}

}

func (PM ProcessManager) ExportDBs(wg *sync.WaitGroup,
	selectedTableRow []int, colnames []string, expresults map[string]chan db.Table) {
	for _, database := range PM.databases {
		go PM.exporter.Export(wg, selectedTableRow, colnames, database.Name,
			expresults[database.Name])
	}
}

func (PM ProcessManager) ShowDBs(wg *sync.WaitGroup, represults map[string]chan db.Table) {
	for _, database := range PM.databases {
		go PM.reporter.ShowTableInfo(wg, represults[database.Name])
	}

}

func (PM ProcessManager) GetDatabaseNames() []string {
	var databaseNames []string
	for _, db := range PM.databases {
		databaseNames = append(databaseNames, db.Name)
	}
	return databaseNames
}

func (PM ProcessManager) ShowInfo(selectedPage int, filterlop string) {
	for _, database := range PM.databases {
		PM.reporter.ShowPageInfo(database, uint32(selectedPage))
		PM.reporter.ShowLDFInfo(database, filterlop)
	}
}
