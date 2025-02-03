package main

import (
	"MSSQLParser/db"
	"MSSQLParser/exporter"
	"MSSQLParser/reporter"
	"fmt"
	"path/filepath"
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
	showTableIndex bool, showPageStats bool, showIndex bool, selectedTableRows int,
	skippedTableRows int, selectedTableRow int, showcarved bool,
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
		SelectedTableRows:   selectedTableRows,
		SkippedTableRows:    skippedTableRows,
		SelectedTableRow:    selectedTableRow,
		ShowCarved:          showcarved,
		ShowLDF:             showLDF,
		TableType:           tabletype,
		Raw:                 raw,
		ShowColNames:        colnames}

	PM.exporter = exporter.Exporter{Format: exportFormat, Image: exportImage, Path: exportPath}
}

func (PM *ProcessManager) ProcessDBFiles(mdffiles []string, ldffiles []string,
	selectedPage int, fromPage int, toPage int, ldf bool, showcarved bool) {

	var database db.Database

	for idx, inputFile := range mdffiles {
		if len(ldffiles) > 0 {
			database = db.Database{Fname: inputFile, Name: filepath.Base(inputFile), Lname: ldffiles[idx]}
		} else {
			database = db.Database{Fname: inputFile, Name: filepath.Base(inputFile)}
		}

		/*processing pages stage */
		totalProcessedPages := database.ProcessMDF(selectedPage, fromPage, toPage, showcarved)
		if totalProcessedPages == -1 {
			continue
		}

		fmt.Printf("Processed %d pages.\n", totalProcessedPages)

		if totalProcessedPages <= 0 {
			fmt.Printf("no pages found skipped processing\n")
			continue
		}

		database.ProcessSystemTables()
		fmt.Printf("Processed system tables \n")

		if ldf {
			ldfRecordsProcessed, err := database.ProcessLDF()

			if err != nil && ldfRecordsProcessed > 0 {
				database.LocateLogRecords()
			}

		}

		PM.databases = append(PM.databases, database)
	}

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

func (PM ProcessManager) ProcessDBTables(tablenames []string, tabletype string, tablepages []int,
	selectedTableRow int, colnames []string) {

	fmt.Println("Table Reconstruction - Report - Export Stage")

	for _, database := range PM.databases {

		/*retrieving schema and table contents */

		represults := make(chan db.Table, 100) //max number of tables for report
		expresults := make(chan db.Table, 100)

		wg := new(sync.WaitGroup)
		wg.Add(3)

		go database.ProcessTables(wg, tablenames, tabletype, represults, expresults, tablepages)
		go PM.reporter.ShowTableInfo(wg, represults)

		go PM.exporter.Export(wg, selectedTableRow, colnames, database.Name, expresults)
		wg.Wait()

	}

}

func (PM ProcessManager) ShowInfo(selectedPage int, filterlop string) {
	for _, database := range PM.databases {
		PM.reporter.ShowPageInfo(database, uint32(selectedPage))
		PM.reporter.ShowLDFInfo(database, filterlop)
	}
}
