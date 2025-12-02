package manager

import (
	"MSSQLParser/channels"
	"MSSQLParser/db"
	"MSSQLParser/exporter"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/reporter"
	"MSSQLParser/utils"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
)

var CHANNEL_SIZE = 100000

type ProcessManager struct {
	reporter           reporter.Reporter
	exporter           exporter.Exporter
	Databases          []db.Database
	TableConfiguration TableProcessorConfiguration
	BroadcastService   channels.BroadcastServer
}

type TableProcessorConfiguration struct {
	SelectedTables  []string
	SelectedColumns []string
	SelectedPages   []int
	SelectedType    string
}

func (PM *ProcessManager) Initialize(showGamExtents bool, showSGamExtents bool, showIAMExtents bool,
	showDataCols bool, showPFS bool, showHeader bool, showSlots bool, showTableSchema bool,
	showTableContent bool, showTableAllocation string,
	showTableIndex bool, showPageStats bool, showIndex bool, toTableRow int,
	skippedTableRows int, selectedTableRows []int, showcarved bool, showTableLDF bool,
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
		ShowTableLDF:        showTableLDF,
		ShowLDF:             showLDF,
		TableType:           tabletype,
		Raw:                 raw,
		ShowColNames:        colnames}

	PM.exporter = exporter.Exporter{Format: exportFormat, Image: exportImage, Path: exportPath}

}

func (PM *ProcessManager) SetExportPath(path string) {
	PM.exporter.Path = path
}

func (PM *ProcessManager) SetShowCarve(showcarve bool) {
	PM.reporter.ShowCarved = showcarve
}

func (PM *ProcessManager) ProcessDBFiles(mdffiles []string, ldffiles []string,
	selectedPages []int, fromPage int, toPage int, carve bool) int {

	var database db.Database

	processedPages := 0
	// ensure one to one match
	slices.SortFunc(mdffiles, utils.RemoveID)
	slices.SortFunc(ldffiles, utils.RemoveID)

	for idx, inputFile := range mdffiles {
		if len(ldffiles) > 0 {
			database = db.Database{Fname: inputFile, Lname: ldffiles[idx]}
		} else {
			database = db.Database{Fname: inputFile}
		}

		/*processing pages stage */
		totalProcessedPages, err := database.ProcessMDF(selectedPages, fromPage, toPage, carve)
		if err != nil {
			continue
		}

		if totalProcessedPages == 0 {
			fmt.Printf("no pages found skipped processing\n")
			continue
		}

		database.ProcessSystemTables()

		ldfRecordsProcessed, err := database.ProcessLDF(carve)

		if err == nil && ldfRecordsProcessed > 0 {
			database.AddLogRecords(carve)
		}

		processedPages += totalProcessedPages
		PM.Databases = append(PM.Databases, database)
	}
	return processedPages

}

func (PM *ProcessManager) FilterDatabases(pageType string, systemTables string, userTable string) {
	for dbidx := range PM.Databases {
		if pageType != "" {
			PM.Databases[dbidx].FilterPagesByType(pageType) //mutable

		}

		if systemTables != "" {
			PM.Databases[dbidx].FilterPagesBySystemTables(systemTables)

		}

		if userTable != "" {
			PM.Databases[dbidx].FilterPagesBySystemTables("sysschobjs")
		}
	}

}

func (PM ProcessManager) ProcessTables(selectedTables []int) {

	for _, database := range PM.Databases {
		wg := new(sync.WaitGroup)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var listener1, listener2 <-chan db.Table
		tablesCH := make(chan db.Table, CHANNEL_SIZE)
		broadcaster := channels.NewBroadcastServer(ctx, tablesCH)

		msg := fmt.Sprintf("Processing tables of database %s from %s ", database.Name, database.Fname)
		fmt.Printf("%s \n", msg)
		mslogger.Mslogger.Info(msg)

		if PM.exporter.Path != "" {

			listener1 = broadcaster.Subscribe()

		}

		listener2 = broadcaster.Subscribe()

		go database.ProcessTables(ctx, PM.TableConfiguration.SelectedTables, PM.TableConfiguration.SelectedType,
			tablesCH, PM.TableConfiguration.SelectedPages)

		if PM.exporter.Path != "" {
			wg.Add(1)
			lastDBFolder := filepath.Base(filepath.Clean(database.Fname))
			lastDBFolder = lastDBFolder[:len(lastDBFolder)-len(filepath.Ext(lastDBFolder))]
			go PM.exporter.Export(wg, selectedTables, PM.TableConfiguration.SelectedColumns, database.Name, lastDBFolder,
				listener1)
		}
		wg.Add(1)
		go PM.reporter.ShowTableInfo(wg, listener2)
		wg.Wait()
	}

}

func (PM ProcessManager) GetDatabaseNames() []string {
	var databaseNames []string
	for _, db := range PM.Databases {
		databaseNames = append(databaseNames, db.Name)
	}
	return databaseNames
}

func (PM ProcessManager) ShowInfo(selectedPages []uint32, filterlop string) {
	for _, database := range PM.Databases {
		PM.reporter.ShowPageInfo(database, selectedPages)
		PM.reporter.ShowLDFInfo(database, selectedPages, filterlop)

	}
}

func (PM ProcessManager) NewBroadcastServer(ctx context.Context, source <-chan int) {

}
