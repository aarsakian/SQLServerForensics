package manager

import (
	"MSSQLParser/channels"
	"MSSQLParser/db"
	"MSSQLParser/exporter"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/reporter"
	"context"
	"fmt"
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

func (PM *ProcessManager) ProcessDBFiles(mdffiles []string, ldffiles []string,
	selectedPage int, fromPage int, toPage int, ldfLevel int, carve bool) int {

	var database db.Database

	processedPages := 0
	for idx, inputFile := range mdffiles {
		if len(ldffiles) > 0 {
			database = db.Database{Fname: inputFile, Lname: ldffiles[idx]}
		} else {
			database = db.Database{Fname: inputFile}
		}

		/*processing pages stage */
		totalProcessedPages, err := database.ProcessMDF(selectedPage, fromPage, toPage, carve)
		if err != nil {
			continue
		}

		if totalProcessedPages == 0 {
			fmt.Printf("no pages found skipped processing\n")
			continue
		}

		database.ProcessSystemTables()

		if ldfLevel == 1 || ldfLevel == 2 {
			ldfRecordsProcessed, err := database.ProcessLDF(carve)

			if err == nil && ldfRecordsProcessed > 0 {
				database.AddLogRecords(carve)
			}

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

func (PM ProcessManager) ProcessTables(selectedTables []int, ldfLevel int) {

	for _, database := range PM.Databases {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		srcCH := make(chan db.Table, CHANNEL_SIZE)
		broadcaster := channels.NewBroadcastServer(ctx, srcCH)
		listener1 := broadcaster.Subscribe()
		listener2 := broadcaster.Subscribe()

		msg := fmt.Sprintf("table contents of %s ", database.Name)
		fmt.Printf("%s \n", msg)
		mslogger.Mslogger.Info(msg)
		wg := new(sync.WaitGroup)
		wg.Add(2)
		go database.ProcessTables(ctx, PM.TableConfiguration.SelectedTables, PM.TableConfiguration.SelectedType,
			srcCH, PM.TableConfiguration.SelectedPages, ldfLevel)

		go PM.exporter.Export(wg, selectedTables, PM.TableConfiguration.SelectedColumns, database.Name,
			listener1)

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

func (PM ProcessManager) ShowInfo(selectedPage int, filterlop string) {
	for _, database := range PM.Databases {
		PM.reporter.ShowPageInfo(database, uint32(selectedPage))
		PM.reporter.ShowLDFInfo(database, filterlop)
	}
}

func (PM ProcessManager) NewBroadcastServer(ctx context.Context, source <-chan int) {

}
