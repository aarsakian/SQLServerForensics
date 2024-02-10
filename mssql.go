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
	"MSSQLParser/utils"
	"flag"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"time"

	disk "github.com/aarsakian/MFTExtractor/Disk"
	"github.com/aarsakian/MFTExtractor/FS/NTFS/MFT"
	MFTExporter "github.com/aarsakian/MFTExtractor/exporter"
	MFTExtractorLogger "github.com/aarsakian/MFTExtractor/logger"
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
	showLDF := flag.Bool("showldf", false, "show vlf, log blocks and records of ldf files")
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
	low := flag.Bool("low", false, "copy MDF file using low level access. Use location flag to set destination.")
	ldf := flag.Bool("ldf", false, "parse hardened (commited) transactions saved to the log")
	filterlop := flag.String("filterlop", "", "filter log records per lop type values are insert|begin|commit|any")

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
		ShowLDF:             *showLDF,
		TableType:           *tabletype}

	var physicalDisk disk.Disk
	var recordsPerPartition map[int]MFT.Records
	var mdffiles, ldffiles []string

	var ldffile, mdffile, basepath string
	var database db.Database

	var e error

	if *stopService {
		servicer.StopService()
		defer servicer.StartService()
	}

	if *inputfile != "" {
		basepath, mdffile = filepath.Split(*inputfile)
		if filepath.IsAbs(basepath) {
			//remove C:\\ and last \\
			basepath = basepath[3 : len(basepath)-1]
		}

		if *ldf {
			ldffile, e = utils.LocateLDFfile(*inputfile)
			if e != nil {
				mslogger.Mslogger.Error(e)
			}
		}
	}

	exp := MFTExporter.Exporter{Location: *location, Hash: "SHA1"}

	if *evidencefile != "" || *physicalDrive != -1 || *vmdkfile != "" ||
		*low && *inputfile != "" {

		if *evidencefile != "" {
			physicalDisk = disk.InitiliazeEvidence(*evidencefile)
		} else if *physicalDrive != -1 {
			physicalDisk = disk.InitializePhysicalDisk(*physicalDrive)
		} else if *low {
			physicalDisk = disk.InitializePhysicalDisk(0)
		} else if *vmdkfile != "" {
			physicalDisk = disk.InitalizeVMDKDisk(*vmdkfile)
		}

		defer physicalDisk.Close()
		physicalDisk.DiscoverPartitions()

		physicalDisk.ProcessPartitions(*partitionNum, []int{}, -1, math.MaxUint32)
		recordsPerPartition = physicalDisk.GetFileSystemMetadata(*partitionNum)

		for partitionId, records := range recordsPerPartition {
			if len(records) == 0 {
				continue
			}

			if len(mdffile) != 0 && len(ldffile) != 0 {

				records = records.FilterByNames([]string{mdffile, ldffile})
			} else if len(mdffile) != 0 {
				records = records.FilterByName(mdffile)
			} else if *ldf {
				records = records.FilterByExtensions([]string{"MDF", "LDF"})
			} else {
				records = records.FilterByExtensions([]string{"MDF"})
			}

			if len(basepath) != 0 {
				records = records.FilterByPath(basepath)
			}

			exp.ExportRecords(records, physicalDisk, partitionId)

			for _, record := range records {
				fullpath := filepath.Join(exp.Location, record.GetFname())
				extension := path.Ext(fullpath)
				if extension == ".mdf" {
					mdffiles = append(mdffiles, fullpath)
				} else if extension == ".ldf" {
					ldffiles = append(ldffiles, fullpath)
				}

			}

		}

	} else if *inputfile != "" {
		mdffiles = append(mdffiles, *inputfile)
		if *ldf {

			ldffiles = append(ldffiles, ldffile)

		}
	}

	for idx, inputFile := range mdffiles {

		if len(ldffiles) != len(mdffiles) {
			database = db.Database{Fname: inputFile}
		} else {
			database = db.Database{Fname: inputFile, Lname: ldffiles[idx]}
		}
		/*processing pages stage */
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

		/*retrieving schema and table contents */
		database.GetTables(*tableName)

		fmt.Printf("Reconstructed %d tables.\n", len(database.Tables))
		fmt.Println("Reporting & exporting stage.")

		reporter.ShowPageInfo(database, uint32(*selectedPage))
		reporter.ShowTableInfo(database)
		reporter.ShowLDFInfo(database, *filterlop)

		if *exportPath != "" {
			exp := exporter.Exporter{Format: *exportFormat, Image: *exportImage, Path: *exportPath}
			exp.Export(database, *tableName, *tabletype, *selectedTableRow)
		}
	}

}
