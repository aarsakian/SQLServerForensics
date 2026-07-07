package manager

import (
	"MSSQLParser/channels"
	"MSSQLParser/correlation"
	"MSSQLParser/db"
	"MSSQLParser/exporter"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/reporter"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aarsakian/FileSystemForensics/utils"
)

var CHANNEL_SIZE = 100000

type ProcessManager struct {
	reporter           reporter.Reporter
	Exporter           exporter.Exporter
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
	exportFormat string, exportBlob bool, exportPath string, exportIndex bool,
	exportSchema bool,
	sortByLSN string,
	walkpageLSN string, walkLSN string) {

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
		WalkPageLSN:         walkpageLSN,
		WalkLSN:             walkLSN,
	}

	PM.Exporter = exporter.Exporter{Format: exportFormat,
		Blobs: exportBlob, Path: exportPath, Index: exportIndex, Schema: exportSchema}

}

func (PM *ProcessManager) SetExportPath(path string) {
	PM.Exporter.Path = path
}

func (PM *ProcessManager) SetShowCarve(showcarve bool) {
	PM.reporter.ShowCarved = showcarve
}

func (PM *ProcessManager) ProcessBAKFiles(bakPayloads []string) int {
	var database db.Database
	totalProcessedPages := 0
	PM.Databases = make(map[string]db.Database)
	for _, bakPayload := range bakPayloads {
		database = db.Database{BakName: bakPayload}
		processedPages, err := database.ProcessBAK(false)
		if err != nil {
			continue
		}
		totalProcessedPages += processedPages
		database.ProcessSystemTables()
		database.LinkPartitionIdToObjectId()
		PM.Databases[utils.StringifyGUID(database.BindingID[:])] = database
	}
	return totalProcessedPages

}

func (PM *ProcessManager) ProcessDBFiles(mdffiles []string, ldffiles []string,
	selectedPages []int, fromPage int, toPage int, carve bool) int {

	var dbkey string

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
		database.LinkPartitionIdToObjectId()

		processedPages += totalProcessedPages

		dir, _ := filepath.Split(inputFile)

		keyb := append([]byte(dir), database.DbiCheckptLSN.ToBytes()...)
		sum := md5.Sum(keyb)

		key := fmt.Sprintf("%s_%s", database.GetBindingID(), hex.EncodeToString(sum[:]))

		PM.Databases[key] = database
	}

	for _, inputFile := range ldffiles {
		dir, _ := filepath.Split(inputFile)

		logdb := new(db.LogDB)
		ldfRecordsProcessed, err := logdb.ProcessLDF(inputFile, carve)
		if err != nil {
			fmt.Printf("skipping processing of ldf file %s due to error %s \n", inputFile, err)
			continue

		} else {
			fmt.Printf("processed %d log records from ldf file %s \n", ldfRecordsProcessed, inputFile)

		}

		calculatedDbiCheckptLsn, err2 := logdb.GetDbiCheckptLSN()
		if err2 != nil {
			fmt.Printf("skipping processing of ldf file %s due to error getting calculated DbiCheckpt LSN %s \n", inputFile, err2)
			continue
		}

		minLsn, _ := logdb.GetMinLSN(calculatedDbiCheckptLsn)

		keyb := append([]byte(dir), calculatedDbiCheckptLsn.ToBytes()...)
		sum := md5.Sum(keyb)

		dbkey = fmt.Sprintf("%s_%s", logdb.GetBindingID(), hex.EncodeToString(sum[:]))

		database, ok := PM.Databases[dbkey]

		//located db sharing the same binding id and directory with the ldf file
		if ok {

			database.UpdateState(minLsn)

			database.LogDB = logdb
			database.Lname = inputFile

			//need to confirm that the checkpoint LSN is valid before adding log records to database and correlating with pages

			database.AddLogRecords()
			database.CorrelateLDFToPages()

			PM.Databases[dbkey] = database

		} else {
			for _, database := range PM.Databases {
				//already matched skip
				if database.Lname != "" {
					continue
				}
				//same database
				dbdir, _ := filepath.Split(database.Fname)
				if database.GetBindingID() == logdb.GetBindingID() && dir == dbdir {
					logdb.LocateDirtyPages(database.DbiCheckptLSN)
					fmt.Printf(`cannot locate last completed checkpoint LSN %s in log file %s for database %s \n`,
						database.DbiCheckptLSN.ToStr(), inputFile, database.Name)
				}

			}

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

func (PM *ProcessManager) ProcessTables() {

	for guid, database := range PM.Databases {
		wg := new(sync.WaitGroup)
		ctx, cancel := context.WithCancel(context.Background())

		var listener2, listener3 <-chan *db.Table
		tablesCH := make(chan *db.Table, CHANNEL_SIZE)

		broadcaster := channels.NewBroadcastServer(ctx, tablesCH)

		if database.Tables == nil {
			database.Tables = make([]*db.Table, 0, len(database.GetTablesInfo()))
		}

		msg := fmt.Sprintf("Processing tables of database %s from %s ", database.Name, database.Fname)
		fmt.Printf("%s \n", msg)
		mslogger.Mslogger.Info(msg)

		listener2 = broadcaster.Subscribe()
		listener3 = broadcaster.Subscribe()

		go database.ProcessTables(ctx, PM.TableConfiguration.SelectedTables, PM.TableConfiguration.SelectedType,
			tablesCH, PM.TableConfiguration.SelectedPages)

		wg.Add(1)
		go func(wgs *sync.WaitGroup) {
			defer wgs.Done()
			for table := range listener2 {

				database.Tables = append(database.Tables, table)

			}

			PM.Databases[guid] = database
		}(wg)

		wg.Add(1)
		go PM.reporter.ShowTableInfo(wg, listener3)
		wg.Wait()
		cancel()
	}

}

func (PM *ProcessManager) ExportTables(selectedTableRows []int) {
	if PM.Exporter.Path == "" {
		return
	}

	for _, database := range PM.Databases {
		tablesCH := make(chan *db.Table, len(database.Tables))
		for _, table := range database.Tables {
			tablesCH <- table
		}
		close(tablesCH)

		wg := new(sync.WaitGroup)
		wg.Add(1)
		databaseFolder := ""
		sourceFilename := filepath.Base(database.Fname)
		dbFolderName := strings.TrimSuffix(sourceFilename, filepath.Ext(sourceFilename))
		go PM.Exporter.Export(wg, selectedTableRows, PM.TableConfiguration.SelectedColumns,
			dbFolderName, sourceFilename, databaseFolder, database.Name, tablesCH)
		wg.Wait()
	}
}

func (PM *ProcessManager) Correlate() {
	for guid, database := range PM.Databases {
		if database.LogDB == nil {
			continue
		}
		CorrelationEngine := new(correlation.CorrelationEngine)
		CorrelationEngine.CorrelateRecords(database.LogDB.GetRecords(), database.PartitionIdToObjectID)
		for _, table := range database.Tables {

			correlatedRecords := CorrelationEngine.CorrelateTable(table.ObjectId)

			table.AddChangesHistory(correlatedRecords, database.PagesPerAllocUnitID)

		}

		PM.Databases[guid] = database
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
