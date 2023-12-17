//page size 8KB
//extend = 8 contiguous pages (64KB)
//data row offsets at the end of the page sequentially in reverse byte
//offset from the beginning of the page

/*GAM and SGAM pages. The first GAM page is always the third page in
the data file (page number 2). The first SGAM page is always the fourth page in the data file (page number 3).
The next GAM and SGAM pages appear every 511,230 pages in the data files, which allows SQL Server to navigate
through them quickly when needed.
GAM SGAM
1    0 Free
0    0 No Free extend
0    1 Mixed with free pages
*/
/*Every PFS page tracks 8,088 pages, or about 64 MB of data space. It is always the second page (page 1) in
the file and every 8,088 pages thereafter. Every pffstatus byte tracks info about a page
Index allocation map (IAM) pages keep track of the extents used by a heap or index.

*/

package main

import (
	"MSSQLParser/db"
	"MSSQLParser/exporter"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/reporter"
	"MSSQLParser/servicer"
	"flag"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"time"

	disk "github.com/aarsakian/MFTExtractor/Disk"
	"github.com/aarsakian/MFTExtractor/FS/NTFS/MFT"
	MFTExporter "github.com/aarsakian/MFTExtractor/exporter"
	"github.com/aarsakian/MFTExtractor/img"
	MFTExtractorLogger "github.com/aarsakian/MFTExtractor/logger"
	"github.com/aarsakian/MFTExtractor/utils"
	VMDKLogger "github.com/aarsakian/VMDK_Reader/logger"
)

func main() {

	inputfile := flag.String("db", "", "absolute path to the MDF file")
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
	tableName := flag.String("table", "", "show table (use all for all tables)")
	showTableContent := flag.Bool("showcontent", false, "show table contents")
	showTableSchema := flag.Bool("showschema", false, "show table schema")
	showGamExtents := flag.Bool("gam", false, "show GAM extents for each page")
	showSGamExtents := flag.Bool("sgam", false, "show SGAM extents for each page")
	showIAMExtents := flag.Bool("iam", false, "show IAM extents for each page")
	showDataCols := flag.Bool("datacols", false, "show data cols for each data row")
	showSlots := flag.Bool("slots", false, "show page slots")
	showPFS := flag.Bool("pfs", false, "show pfm page allocation")
	showIndex := flag.Bool("showindex", false, "show index contents")
	showTableAllocation := flag.String("showTableAllocation", "",
		"show pages that the table has been allocated write 'simple' or 'links' to see the linked page structure")
	selectedTableRows := flag.Int("torow", -1, "show only the first number of rows (Default is all)")
	selectedTableRow := flag.Int("row", -1, "Show only the selected row")
	userTable := flag.String("usertable", "", "get system table info about user table")
	exportPath := flag.String("export", "", "export table")
	exportFormat := flag.String("format", "csv", "select format to export (csv)")
	logactive := flag.Bool("log", false, "log activity")
	tabletype := flag.String("tabletype", "", "filter tables by type (xtype) e.g. user for user tables")
	exportImage := flag.Bool("exportImages", false, "export images saved as blob")
	stopService := flag.Bool("stopservice", false, "stop MSSQL service (requires admin rights!)")

	flag.Parse()

	now := time.Now()
	logfilename := "logs" + now.Format("2006-01-02T15_04_05") + ".txt"
	mslogger.InitializeLogger(*logactive, logfilename)
	MFTExtractorLogger.InitializeLogger(*logactive, logfilename)
	VMDKLogger.InitializeLogger(*logactive, logfilename)

	reporter := reporter.Reporter{ShowGamExtents: *showGamExtents,
		ShowSGamExtents:     *showSGamExtents,
		ShowIAMExtents:      *showIAMExtents,
		ShowDataCols:        *showDataCols,
		ShowPFS:             *showPFS,
		ShowHeader:          *showHeader,
		ShowSlots:           *showSlots,
		TableName:           *tableName,
		ShowTableSchema:     *showTableSchema,
		ShowTableContent:    *showTableContent,
		ShowTableAllocation: *showTableAllocation,
		ShowPageStats:       *showPageStats,
		ShowIndex:           *showIndex,
		SelectedTableRows:   *selectedTableRows,
		SelectedTableRow:    *selectedTableRow,
		ShowCarved:          *showcarved,
		TableType:           *tabletype}

	var hD img.DiskReader

	var physicalDisk disk.Disk
	var recordsPerPartition map[int]MFT.Records
	var inputfiles []string

	if *stopService {
		servicer.StopService()
		defer servicer.StartService()
	}

	if *evidencefile != "" || *physicalDrive != -1 {

		if *physicalDrive != -1 {

			hD = img.GetHandler(fmt.Sprintf("\\\\.\\PHYSICALDRIVE%d", *physicalDrive), "physicalDrive")
		} else if *evidencefile != "" {
			hD = img.GetHandler(*evidencefile, "ewf")

		} else if *vmdkfile != "" {
			hD = img.GetHandler(*vmdkfile, "vmdk")
		}
		physicalDisk = disk.Disk{Handler: hD}
		physicalDisk.DiscoverPartitions()

		physicalDisk.ProcessPartitions(*partitionNum, []int{}, -1, math.MaxUint32)
		recordsPerPartition = physicalDisk.GetFileSystemMetadata(*partitionNum)

		defer hD.CloseHandler()
		exp := MFTExporter.Exporter{Location: *location, Hash: "SHA1"}
		for partitionId, records := range recordsPerPartition {

			records = records.FilterByExtension("MDF")
			if len(records) == 0 {
				continue
			}
			if *location != "" {

				results := make(chan utils.AskedFile, len(records))

				wg := new(sync.WaitGroup)
				wg.Add(2)

				go exp.ExportData(wg, results)                            //consumer
				go physicalDisk.Worker(wg, records, results, partitionId) //producer

				wg.Wait()
				exp.SetFilesToLogicalSize(records)

			}

			exp.HashFiles(records)

			for _, record := range records {
				fullpath := filepath.Join(exp.Location, record.GetFname())
				inputfiles = append(inputfiles, fullpath)
			}

		}

	}

	if *inputfile != "" {
		inputfiles = append(inputfiles, *inputfile)
	}
	for _, inputFile := range inputfiles {
		fmt.Printf("about to process database file %s \n", inputFile)
		database := db.Database{Fname: inputFile}

		totalProcessedPages := database.Process(*selectedPage, *fromPage, *toPage, *showcarved)

		if *pageType != "" {
			database.FilterPagesByType(*pageType) //mutable

		}

		if *systemTables != "" {
			database.FilterPagesBySystemTables(*systemTables)

		}

		if *userTable != "" {
			database.FilterPagesBySystemTables("sysschobjs")
		}

		fmt.Printf("Processed %d pages.\n", totalProcessedPages)
		fmt.Println("Reconstructing tables...")

		tables := database.GetTablesInformation(*tableName)
		database.Tables = tables

		fmt.Printf("Reconstructed %d tables.\n", len(tables))
		fmt.Println("Reporting & exporting stage.")

		reporter.ShowPageInfo(database, uint32(*selectedPage))
		reporter.ShowTableInfo(database)

		if *exportPath != "" {
			exp := exporter.Exporter{Format: *exportFormat, Image: *exportImage, Path: *exportPath}
			exp.Export(database, *tableName, *tabletype, *selectedTableRow)
		}
	}

}
