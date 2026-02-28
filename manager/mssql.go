package manager

import (
	"MSSQLParser/channels"
	"MSSQLParser/db"
	"MSSQLParser/exporter"
	"MSSQLParser/logger"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/reporter"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/aarsakian/FileSystemForensics/utils"
)

var CHANNEL_SIZE = 100000

type ProcessManager struct {
	reporter           reporter.Reporter
	exporter           exporter.Exporter
	Databases          map[string]db.Database
	TableConfiguration TableProcessorConfiguration
	BroadcastService   channels.BroadcastServer
}

type TableProcessorConfiguration struct {
	SelectedTables  []string
	SelectedColumns []string
	SelectedPages   []int
	SelectedType    string
}

func (PM *ProcessManager) Initialize(showDBInfo bool, showGamExtents bool, showSGamExtents bool,
	showIAMExtents bool,
	showDataCols bool, showPFS bool, showBCM bool,
	showDiffMap bool, showHeader bool, showSlots bool, showTableSchema bool,
	showTableContent bool, showTableAllocation string,
	showTableIndex bool, showPageStats bool, showIndex bool, toTableRow int,
	skippedTableRows int, selectedTableRows []int, showcarved bool, showTableLDF bool,
	showLDF bool, tabletype string, raw bool, colnames []string,
	exportFormat string, exportImage bool, exportPath string, sortByLSN string,
	walkLSN string) {

	PM.reporter = reporter.Reporter{
		ShowDBInfo:          showDBInfo,
		ShowGamExtents:      showGamExtents,
		ShowSGamExtents:     showSGamExtents,
		ShowIAMExtents:      showIAMExtents,
		ShowDataCols:        showDataCols,
		ShowPFS:             showPFS,
		ShowBCMExtents:      showBCM,
		ShowDiffMapExtents:  showDiffMap,
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
		ShowColNames:        colnames,
		SortByLSN:           sortByLSN,
		WalkLSN:             walkLSN,
	}

	PM.exporter = exporter.Exporter{Format: exportFormat, Image: exportImage, Path: exportPath}

}

func (PM *ProcessManager) SetExportPath(path string) {
	PM.exporter.Path = path
}

func (PM *ProcessManager) SetShowCarve(showcarve bool) {
	PM.reporter.ShowCarved = showcarve
}

func (PM *ProcessManager) ProcessBAKFiles(bakPayloads []string) int {
	var database db.Database
	totalProcessedPages := 0

	for _, bakPayload := range bakPayloads {
		database = db.Database{BakName: bakPayload}
		processedPages, err := database.ProcessBAK(false)
		if err != nil {
			continue
		}
		totalProcessedPages += processedPages
		database.ProcessSystemTables()
		PM.Databases[utils.StringifyGUID(database.BindingID[:])] = database
	}
	return totalProcessedPages

}

func (PM *ProcessManager) ProcessDBFiles(mdffiles []string, ldffiles []string,
	selectedPages []int, fromPage int, toPage int, carve bool) int {

	processedPages := 0
	// ensure one to one match
	PM.Databases = make(map[string]db.Database)
	for _, inputFile := range mdffiles {

		database := db.Database{Fname: inputFile}

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

		processedPages += totalProcessedPages
		requestedDB, ok := PM.Databases[database.GetBindingID()]
		if ok {
			logdb := requestedDB.LogDB
			database.LogDB = logdb

		}
		dir, _ := filepath.Split(inputFile)
		sum := md5.Sum([]byte(dir))

		key := fmt.Sprintf("%s_%s", database.GetBindingID(), hex.EncodeToString(sum[:]))

		PM.Databases[key] = database
	}

	for _, inputFile := range ldffiles {
		dir, _ := filepath.Split(inputFile)

		logdb := new(db.LogDB)
		ldfRecordsProcessed, err := logdb.ProcessLDF(inputFile, carve)
		fmt.Printf("processed %d log records from ldf file %s \n", ldfRecordsProcessed, inputFile)
		if err == nil {
			sum := md5.Sum([]byte(dir))

			key := fmt.Sprintf("%s_%s", logdb.GetBindingID(), hex.EncodeToString(sum[:]))
			database, ok := PM.Databases[key]

			if ok {
				database.LogDB = logdb
				database.Lname = inputFile

			} else {
				database = db.Database{Lname: inputFile, LogDB: logdb}

			}

			if database.NofPages == 0 {
				logger.Mslogger.Warning(fmt.Sprintf("no database for log file %s", database.Lname))
				continue
			}
			fmt.Printf("Updating log records - Correlating log records with database.\n")
			database.AddLogRecords(carve)
			database.UpdateLogRecordStatus()
			database.CorrelateLDFToPages()

			PM.Databases[key] = database
		}
	}

	return processedPages

}

func (PM *ProcessManager) FilterDatabases(pageType string, systemTables string, userTable string) {
	for guid, database := range PM.Databases {
		if pageType != "" {
			database.FilterPagesByTypeMutable(pageType) //mutable

		}

		if systemTables != "" {
			database.FilterPagesBySystemTables(systemTables)

		}

		if userTable != "" {
			database.FilterPagesBySystemTables("sysschobjs")
		}
		PM.Databases[guid] = database
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
		PM.reporter.ShowPageInfo(database, selectedPages, filterlop)
		PM.reporter.ShowLDFInfo(database, selectedPages, filterlop)

	}
}

func (PM ProcessManager) NewBroadcastServer(ctx context.Context, source <-chan int) {

}
