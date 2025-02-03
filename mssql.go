package main

import (
	"MSSQLParser/db"
	"MSSQLParser/exporter"
	"MSSQLParser/reporter"
	"fmt"
	"path/filepath"
<<<<<<< HEAD
	"sync"
=======
	"strings"
	"sync"
	"time"

	EWFLogger "github.com/aarsakian/EWF_Reader/logger"
	"github.com/aarsakian/MFTExtractor/disk"
	MFTExporter "github.com/aarsakian/MFTExtractor/exporter"
	"github.com/aarsakian/MFTExtractor/filtermanager"
	"github.com/aarsakian/MFTExtractor/filters"
	MFTExtractorLogger "github.com/aarsakian/MFTExtractor/logger"
	mtfLogger "github.com/aarsakian/MTF_Reader/logger"
	mtf "github.com/aarsakian/MTF_Reader/mtf"
	VMDKLogger "github.com/aarsakian/VMDK_Reader/logger"
>>>>>>> f2e94eb5bb26ad1489150e4e86f1606a16421f78
)

type ProcessManager struct {
	reporter  reporter.Reporter
	exporter  exporter.Exporter
	databases []db.Database
}

<<<<<<< HEAD
func (PM *ProcessManager) Initialize(showGamExtents bool, showSGamExtents bool, showIAMExtents bool,
	showDataCols bool, showPFS bool, showHeader bool, showSlots bool, showTableSchema bool,
	showTableContent bool, showTableAllocation string,
	showTableIndex bool, showPageStats bool, showIndex bool, selectedTableRows int,
	skippedTableRows int, selectedTableRow int, showcarved bool,
	showLDF bool, tabletype string, raw bool, colnames []string,
	exportFormat string, exportImage bool, exportPath string) {
=======
	dbfile := flag.String("db", "", "absolute path to the MDF file")
	ldbfile := flag.String("ldb", "", "absolute path to the LDF file")
	mtffile := flag.String("mtf", "", "path to bak file (TAPE format)")
	physicalDrive := flag.Int("physicaldrive", -1,
		"select the physical disk number to look for MDF file (requires admin rights!)")
	evidencefile := flag.String("evidence", "", "path to image file")
	vmdkfile := flag.String("vmdk", "", "path to vmdk file (Sparse formats are supported)")
	partitionNum := flag.Int("partition", -1,
		"select the partition number to look for MDF files  (requires admin rights!)")
	location := flag.String("location", "MDF", "the path to export  files")
	showcarved := flag.Bool("carve", false, "Carve data records and try to interpret")
	selectedPage := flag.Int("page", -1, "select a page to start parsing")
	fromPage := flag.Int("from", 0, "select page id to start parsing")
	toPage := flag.Int("to", -1, "select page id to end parsing")
	pageType := flag.String("type", "", "filter by page type IAM, GAM, SGAM, PFS, DATA")
	systemTables := flag.String("systemtables", "", "show information about system tables sysschobjs sysrowsets syscolpars")
	showHeader := flag.Bool("header", false, "show page header")
	showPageStats := flag.Bool("showpagestats", false, "show page statistics parses sgam gam and pfm pages")
	tablenames := flag.String("tables", "", "process selectively tables (use comma for each table name)")
	tablepages := flag.String("tablepages", "", "filter rows by pages (use comm)")
	processTables := flag.Bool("processtables", false, "process tables")
	showTableContent := flag.Bool("showcontent", false, "show table contents")
	showTableIndex := flag.Bool("showtableindex", false, "show table index contents")
	showTableSchema := flag.Bool("showschema", false, "show table schema")
	showGamExtents := flag.Bool("gam", false, "show GAM extents for each page")
	showSGamExtents := flag.Bool("sgam", false, "show SGAM extents for each page")
	showIAMExtents := flag.Bool("iam", false, "show IAM extents for each page")
	showDataCols := flag.Bool("datacols", false, "show data cols for each data row")
	showSlots := flag.Bool("slots", false, "show page slots")
	showPFS := flag.Bool("pfs", false, "show pfm page allocation")
	showIndex := flag.Bool("showindex", false, "show index contents")
	showLDF := flag.Bool("showldf", false, "show vlf, log blocks and records of ldf files")
	showTableAllocation := flag.String("showTableAllocation", "",
		"show pages that the table has been allocated write 'simple', 'sorted' or 'links' to see the linked page structure")
	selectedTableRows := flag.Int("torow", -1, "show only the first rows (Default is all)")
	skippedTableRows := flag.Int("fromrow", 0, "show only the last rows (Default is all)")
	selectedTableRow := flag.Int("row", -1, "Show only the selected row")
	userTable := flag.String("usertable", "", "get system table info about user table")
	exportPath := flag.String("export", "", "export table")
	exportFormat := flag.String("format", "csv", "select format to export (csv)")
	logactive := flag.Bool("log", false, "log activity")
	bakactive := flag.Bool("bak", false, "parse bak files found in images")
	tabletype := flag.String("tabletype", "", "filter tables by type e.g. User Table for user tables")
	exportImage := flag.Bool("exportImages", false, "export images saved as blob")
	stopService := flag.Bool("stopservice", false, "stop MSSQL service (requires admin rights!)")
	//	low := flag.Bool("low", false, "copy MDF file using low level access. Use location flag to set destination.")
	ldf := flag.Bool("ldf", false, "parse hardened (commited) transactions saved to the log")
	filterlop := flag.String("filterlop", "", "filter log records per lop type values are insert|begin|commit|any")
	colnames := flag.String("colnames", "", "the columns to display use comma for each column name")
	raw := flag.Bool("raw", false, "show row data for each column in a table")
>>>>>>> f2e94eb5bb26ad1489150e4e86f1606a16421f78

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

<<<<<<< HEAD
	PM.exporter = exporter.Exporter{Format: exportFormat, Image: exportImage, Path: exportPath}
}

func (PM *ProcessManager) ProcessDBFiles(mdffiles []string, ldffiles []string,
	selectedPage int, fromPage int, toPage int, ldf bool, showcarved bool) {

	var database db.Database

=======
	now := time.Now()
	logfilename := "logs" + now.Format("2006-01-02T15_04_05") + ".txt"
	mslogger.InitializeLogger(*logactive, logfilename)
	MFTExtractorLogger.InitializeLogger(*logactive, logfilename)
	VMDKLogger.InitializeLogger(*logactive, logfilename)
	mtfLogger.InitializeLogger(*logactive, logfilename)
	EWFLogger.InitializeLogger(*logactive, logfilename)

	reporter := reporter.Reporter{ShowGamExtents: *showGamExtents,
		ShowSGamExtents:     *showSGamExtents,
		ShowIAMExtents:      *showIAMExtents,
		ShowDataCols:        *showDataCols,
		ShowPFS:             *showPFS,
		ShowHeader:          *showHeader,
		ShowSlots:           *showSlots,
		ShowTableSchema:     *showTableSchema,
		ShowTableContent:    *showTableContent,
		ShowTableAllocation: *showTableAllocation,
		ShowTableIndex:      *showTableIndex,
		ShowPageStats:       *showPageStats,
		ShowIndex:           *showIndex,
		SelectedTableRows:   *selectedTableRows,
		SkippedTableRows:    *skippedTableRows,
		SelectedTableRow:    *selectedTableRow,
		ShowCarved:          *showcarved,
		ShowLDF:             *showLDF,
		TableType:           *tabletype,
		Raw:                 *raw,
		ShowColNames:        strings.Split(*colnames, ",")}

	var mdffiles, ldffiles []string

	var mdffile, basepath string
	var database db.Database

	if *stopService {
		servicer.StopService()
		defer servicer.StartService()
	}

	dbExp := exporter.Exporter{Format: *exportFormat, Image: *exportImage, Path: *exportPath}

	exp := MFTExporter.Exporter{Location: *location, Hash: "SHA1", Strategy: "Id"}

	flm := filtermanager.FilterManager{}

	/*if len(fileNamesToExport) != 0 {
		flm.Register(filters.FoldersFilter{Include: false})
		flm.Register(filters.NameFilter{Filenames: fileNamesToExport})
	}*/

	if *bakactive {
		flm.Register(filters.ExtensionsFilter{Extensions: []string{"bak"}})
	}

	if *ldf {
		flm.Register(filters.ExtensionsFilter{Extensions: []string{"MDF", "LDF"}})

	} else {
		flm.Register(filters.ExtensionsFilter{Extensions: []string{"MDF"}})
	}

	if mdffile != "" && *ldf {
		flm.Register(filters.PrefixesSuffixesFilter{Prefixes: []string{strings.Split(mdffile, ".")[0], strings.Split(mdffile, ".")[0]},
			Suffixes: []string{"ldf", "mdf"}})

	}

	if *dbfile != "" {
		basepath, mdffile = utils.SplitPath(*dbfile)

		if mdffile != "" && !*ldf {

			flm.Register(filters.NameFilter{Filenames: []string{mdffile}})
		}

		if len(basepath) > 0 {
			flm.Register(filters.PathFilter{NamePath: basepath})
		}

	}

	if *evidencefile != "" || *physicalDrive != -1 || *vmdkfile != "" {
		physicalDisk := new(disk.Disk)
		physicalDisk.Initialize(*evidencefile, *physicalDrive, *vmdkfile)

		recordsPerPartition := physicalDisk.Process(*partitionNum, []int{}, -1, math.MaxUint32)
		defer physicalDisk.Close()

		for partitionId, records := range recordsPerPartition {
			if len(records) == 0 {
				continue
			}
			records = flm.ApplyFilters(records)

			records = records.FilterOutDeleted()

			exp.ExportRecords(records, *physicalDisk, partitionId)

			for _, record := range records {

				fullpath := filepath.Join(exp.Location, fmt.Sprintf("[%d]%s", record.Entry, record.GetFname()))
				extension := path.Ext(fullpath)
				if extension == ".mdf" {
					mdffiles = append(mdffiles, fullpath)
				} else if extension == ".ldf" {
					ldffiles = append(ldffiles, fullpath)
				}

			}

		}

	} else if *dbfile != "" {
		mdffiles = append(mdffiles, *dbfile)
		if *ldbfile != "" {
			ldffiles = append(ldffiles, *ldbfile)
		}

	}

	if *mtffile != "" {
		mtf_s := mtf.MTF{Fname: *mtffile}
		mtf_s.Process()
		mtf_s.Export("MDF")
		mdffiles = append(mdffiles, filepath.Join("MDF", mtf_s.GetExportFileName()))
	}

>>>>>>> f2e94eb5bb26ad1489150e4e86f1606a16421f78
	for idx, inputFile := range mdffiles {
		if len(ldffiles) > 0 {
			database = db.Database{Fname: inputFile, Name: filepath.Base(inputFile), Lname: ldffiles[idx]}
		} else {
			database = db.Database{Fname: inputFile, Name: filepath.Base(inputFile)}
		}

		/*processing pages stage */
<<<<<<< HEAD
		totalProcessedPages := database.ProcessMDF(selectedPage, fromPage, toPage, showcarved)
=======
		totalProcessedPages := database.ProcessMDF(*selectedPage, *fromPage, *toPage, *showcarved)
>>>>>>> f2e94eb5bb26ad1489150e4e86f1606a16421f78
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

<<<<<<< HEAD
		if ldf {
			ldfRecordsProcessed, err := database.ProcessLDF()
=======
		if *ldf {
			ldfRecordsProcessed, err := database.ProcessLDF()

			if err != nil && ldfRecordsProcessed > 0 {
				database.LocateRecords()
			}

		}
>>>>>>> f2e94eb5bb26ad1489150e4e86f1606a16421f78

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

<<<<<<< HEAD
		if userTable != "" {
			PM.databases[dbidx].FilterPagesBySystemTables("sysschobjs")
		}
=======
		if !*processTables {
			return
		}
		fmt.Println("Table Reconstruction - Report - Export Stage")

		/*retrieving schema and table contents */

		represults := make(chan db.Table, 100) //max number of tables for report
		expresults := make(chan db.Table, 100)

		wg := new(sync.WaitGroup)
		wg.Add(3)

		go database.ProcessTables(wg, strings.Split(*tablenames, ","), *tabletype, represults, expresults, utils.StringsToIntArray(*tablepages))
		go reporter.ShowTableInfo(wg, represults)

		go dbExp.Export(wg, *selectedTableRow, strings.Split(*colnames, ","), database.Name, expresults)
		wg.Wait()

		reporter.ShowPageInfo(database, uint32(*selectedPage))
		reporter.ShowLDFInfo(database, *filterlop)

>>>>>>> f2e94eb5bb26ad1489150e4e86f1606a16421f78
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
