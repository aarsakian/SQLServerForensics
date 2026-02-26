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
	pb "MSSQLParser/comms"
	msegrpc "MSSQLParser/grpc-server"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/manager"
	"MSSQLParser/servicer"
	"MSSQLParser/utils"
	"log"
	"net"
	"net/http"
	"strconv"

	"flag"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aarsakian/FileSystemForensics/disk"
	FSExporter "github.com/aarsakian/FileSystemForensics/exporter"
	"github.com/aarsakian/FileSystemForensics/filtermanager"
	"github.com/aarsakian/FileSystemForensics/filters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	FSLogger "github.com/aarsakian/FileSystemForensics/logger"
	mtfLogger "github.com/aarsakian/MTF_Reader/logger"
	mtf "github.com/aarsakian/MTF_Reader/mtf"
	VMDKLogger "github.com/aarsakian/VMDK_Reader/logger"

	mssqlparser_comms "MSSQLParser/comms"
)

func main() {

	dbfile := flag.String("db", "", "absolute path to the MDF file")
	ldbfile := flag.String("ldb", "", "absolute path to the LDF file")
	mtffile := flag.String("mtf", "", "path to bak file (TAPE format)")

	physicalDrive := flag.Int("physicaldrive", -1,
		"select the physical disk number to look for MDF file (requires admin rights!)")
	evidencefile := flag.String("evidence", "", "path to image file")
	vmdkfile := flag.String("vmdk", "", "path to vmdk file (Sparse formats are supported)")
	partitionNum := flag.Int("partition", -1,
		"select the partition number to look for MDF files  (requires admin rights!)")
	location := flag.String("location", "MDF", "the path to export MDF/LDF files")
	carve := flag.Bool("carve", false, "Carve data records and try to interpret")
	selectedPages := flag.String("pages", "", "select pages to parse (use comma for each page id, other pages will be ignored)")
	fromPage := flag.Int("from", 0, "select page id to start parsing")
	toPage := flag.Int("to", -1, "select page id to end parsing")
	pageType := flag.String("type", "", "filter by page type IAM, GAM, SGAM, PFS, DATA")

	systemTables := flag.String("systemtables", "", "show information about system tables sysschobjs sysrowsets syscolpars")
	showHeader := flag.Bool("showheader", false, "show page header")
	showPages := flag.String("showpages", "", "select specific pages to show info (use comma for each page id)")
	showPageStats := flag.Bool("showpagestats", false,
		"show page statistics sgam, gam, pfs iam for pages")

	filenames := flag.String("filenames", "", "select mdf files to filter use comma for each file (to be used with evidence)")

	tablenames := flag.String("tables", "", "select the tables to process (use comma for each table name)")
	tablepages := flag.String("tablepages", "", "filter rows by pages (use comma)")
	processTables := flag.Bool("processtables", false, "process tables")

	showDBInfo := flag.Bool("showdbinfo", false, "show database information parsed from fileheader (0 page)")
	showTableContent := flag.Bool("showcontent", false, "show table contents")
	showTableIndex := flag.Bool("showtableindex", false, "show table index contents")
	showTableSchema := flag.Bool("showschema", false, "show table schema")
	showGamExtents := flag.Bool("showgam", false, "show GAM extents for each page")
	showSGamExtents := flag.Bool("showsgam", false, "show SGAM extents for each page")
	showDiffMapExtents := flag.Bool("showdiffmap", false, "show differential map for each page")
	showIAMExtents := flag.Bool("showiam", false, "show IAM extents for each page")
	showDataCols := flag.Bool("showdatacols", false, "show data cols for each data row")
	showSlots := flag.Bool("showslots", false, "show page slots")
	showPFS := flag.Bool("showpfs", false, "show pfs page allocation")
	showIndex := flag.Bool("showindex", false, "show index contents")
	showLDF := flag.Bool("showldf", false, "show vlf, log blocks and records of ldf files")
	showBCM := flag.Bool("showbcm", false, "show bulk difference map")
	showTableLDF := flag.Bool("showtableldf", false, "show table log record info (must be used with table)")
	showTableAllocation := flag.String("showtableallocation", "",
		"show pages that the table has been allocated write 'simple', 'sorted' or 'links' to see the linked page structure")
	toTableRows := flag.Int("torow", -1, "show only the first rows (Default is all)")
	skippedTableRows := flag.Int("fromrow", 0, "show only the last rows (Default is all)")
	selectedTableRows := flag.String("rows", "", "use comma to select rows")
	userTable := flag.String("usertable", "", "get system table info about user table")
	exportPath := flag.String("export", "", "export tables to selected path")
	exportFormat := flag.String("format", "csv", "select format to export (csv)")
	logactive := flag.Bool("log", false, "log activity")
	bakactive := flag.Bool("bak", false, "parse bak files found in images")
	tabletype := flag.String("tabletype", "", "filter tables by type e.g. 'User Table' for user tables 'View' for views")
	exportImage := flag.Bool("exportimages", false, "export blobs (will be exported to a folder images under the database name, file extension is blob)")
	stopService := flag.Bool("stopservice", false, "stop MSSQL service (requires admin rights!)")
	//	low := flag.Bool("low", false, "copy MDF file using low level access. Use location flag to set destination.")
	filterlop := flag.String("filterlop", "", "filter log records per lop type values are insert|begin|commit|begin_ckpt|end_ckpt|any")
	colnames := flag.String("colnames", "", "the columns to display use comma for each column name")
	raw := flag.Bool("showraw", false, "show row data for each column in a table")
	rpc := flag.Uint("rpc", 0, "use grpc to communicate select port from 1024 and upwards")
	walkLSN := flag.String("walklsn", "", "follow lsn allowed values are prev|next")
	sortByLSN := flag.String("sortbylsn", "", "sort pages  all|allocunit (sort all pages or sort per allocation unit basis)")

	profile := flag.Bool("profile", false, "profile memory usage")

	flag.Parse()

	now := time.Now()
	logfilename := "logs" + now.Format("2006-01-02T15_04_05") + ".txt"
	mslogger.InitializeLogger(*logactive, logfilename)
	FSLogger.InitializeLogger(*logactive, logfilename)
	VMDKLogger.InitializeLogger(*logactive, logfilename)
	mtfLogger.InitializeLogger(*logactive, logfilename)

	if *profile {

		go func() {
			log.Println("pprof listening on :6060")
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	var mdffiles, ldffiles, bakfiles, bakPayloads []string

	var mdffile, basepath string

	if *stopService {
		servicer.StopService()
		defer servicer.StartService()
	}

	if *rpc > 1024 && *rpc < 65535 {
		lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *rpc))
		if err != nil {
			log.Fatalf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		pb.RegisterFileProcessorServiceServer(s, &msegrpc.Server{
			ActiveStreams: make(map[string]grpc.BidiStreamingServer[mssqlparser_comms.Message,
				mssqlparser_comms.Message])})

		reflection.Register(s)
		msg := fmt.Sprintf("Listening server at %v", lis.Addr())
		fmt.Printf("%s\n", msg)
		mslogger.Mslogger.Info(msg)

		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to server: %v", err)
		}

	}

	exp := FSExporter.Exporter{Location: *location, Hash: "SHA1", Strategy: "Id", RecreatePath: true}

	flm := filtermanager.FilterManager{}

	/*if len(fileNamesToExport) != 0 {
		flm.Register(filters.FoldersFilter{Include: false})
		flm.Register(filters.NameFilter{Filenames: fileNamesToExport})
	}*/

	if *bakactive {
		flm.Register(filters.ExtensionsFilter{Extensions: []string{"bak"}})
	}

	if *filenames == "" {
		if *ldbfile != "" || *evidencefile != "" || *physicalDrive != -1 || *vmdkfile != "" {
			flm.Register(filters.ExtensionsFilter{Extensions: []string{"MDF", "LDF"}})

		} else {
			flm.Register(filters.ExtensionsFilter{Extensions: []string{"MDF"}})
		}

	}

	if mdffile != "" && *ldbfile != "" {
		flm.Register(filters.PrefixesSuffixesFilter{Prefixes: []string{strings.Split(mdffile, ".")[0], strings.Split(mdffile, ".")[0]},
			Suffixes: []string{"ldf", "mdf"}})

	}

	if *filenames != "" {
		flm.Register(filters.NameFilter{Filenames: strings.Split(*filenames, ",")})
	}

	if *dbfile != "" {
		basepath, mdffile = utils.SplitPath(*dbfile)

		if mdffile != "" && *ldbfile != "" {

			flm.Register(filters.NameFilter{Filenames: []string{mdffile}})
		}

		if len(basepath) > 0 {
			flm.Register(filters.PathFilter{NamePath: basepath})
		}

	}

	if *evidencefile != "" || *physicalDrive != -1 || *vmdkfile != "" {
		physicalDisk := new(disk.Disk)
		physicalDisk.Initialize(*evidencefile, *physicalDrive, *vmdkfile)

		recordsPerPartition, err := physicalDisk.Process(*partitionNum, []int{}, 0, math.MaxUint32)

		defer physicalDisk.Close()

		if err != nil {
			log.Fatal(err)
		}
		for partitionId, records := range recordsPerPartition {
			if len(records) == 0 {
				continue
			}
			records = flm.ApplyFilters(records)

			//	records = records.FilterOutDeleted()

			exp.ExportRecords(records, *physicalDisk, partitionId)

			for _, record := range records {

				fullpath := filepath.Join(exp.Location, record.GetPath(partitionId),
					fmt.Sprintf("[%d]%s",
						record.GetID(), record.GetFname()))
				extension := path.Ext(fullpath)
				switch strings.ToLower(extension) {
				case ".mdf":
					mdffiles = append(mdffiles, fullpath)
				case ".ldf":
					ldffiles = append(ldffiles, fullpath)
				case ".bak":
					bakfiles = append(bakfiles, fullpath)
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
		bakfiles = append(bakfiles, *mtffile)
	}
	if *bakactive || *mtffile != "" {

		for _, bakfile := range bakfiles {
			mtf_s := mtf.MTF{Fname: bakfile}
			mtf_s.Process()
			mtf_s.Export(*location)
			bakPayloads = append(bakPayloads, filepath.Join("MDF", mtf_s.GetExportFileName()))
		}

	}

	var selectedTableRowsInt []int
	for _, val := range strings.Split(*selectedTableRows, ",") {
		val, e := strconv.ParseInt(val, 10, 0)
		if e != nil {
			continue
		}

		selectedTableRowsInt = append(selectedTableRowsInt, int(val))
	}

	pm := manager.ProcessManager{}
	//essential if we want reporting & exporting functionality
	pm.Initialize(*showDBInfo, *showGamExtents, *showSGamExtents, *showIAMExtents,
		*showDataCols, *showPFS, *showBCM, *showDiffMapExtents, *showHeader, *showSlots, *showTableSchema,
		*showTableContent, *showTableAllocation,
		*showTableIndex, *showPageStats, *showIndex, *toTableRows,
		*skippedTableRows, selectedTableRowsInt,
		*carve, *showTableLDF,
		*showLDF, *tabletype, *raw, strings.Split(*colnames, ","),
		*exportFormat, *exportImage, *exportPath, *sortByLSN, *walkLSN)

	pm.TableConfiguration = manager.TableProcessorConfiguration{
		SelectedTables:  strings.Split(*tablenames, ","),
		SelectedType:    *tabletype,
		SelectedPages:   utils.StringsToIntArray(*tablepages),
		SelectedColumns: strings.Split(*colnames, ","),
	}

	start := time.Now()
	processedPages := 0
	if len(bakPayloads) > 0 {
		processedPages = pm.ProcessBAKFiles(bakPayloads)
	} else {
		processedPages = pm.ProcessDBFiles(mdffiles, ldffiles,
			utils.StringsToIntArray(*selectedPages),
			*fromPage, *toPage, *carve)
	}

	fmt.Printf("Processed %d pages %d MB in %f secs \n",
		processedPages, processedPages*8192/1000/1024, time.Since(start).Seconds())

	pm.FilterDatabases(*pageType, *systemTables, *userTable)

	if *processTables {
		start := time.Now()

		pm.ProcessTables(selectedTableRowsInt)

		fmt.Printf("Finished in %f secs", time.Since(start).Seconds())
	}

	pm.ShowInfo(utils.StringsToUint32Array(*showPages), *filterlop)

}
