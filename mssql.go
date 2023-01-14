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
	db "MSSQLParser/db"
	"MSSQLParser/exporter"
	"MSSQLParser/page"
	"MSSQLParser/reporter"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {

	PAGELEN := 8192

	inputfile := flag.String("db", "", "absolute path to the MDF file")
	selectedPage := flag.Int("page", -1, "select a page to start parsing")
	fromPage := flag.Int("from", 0, "select page id to start parsing")
	toPage := flag.Int("to", -1, "select page id to end parsing")
	pageType := flag.String("type", "", "filter by page type IAM, GAM, SGAM, PFS, DATA")
	systemTables := flag.String("systemtables", "", "show information about system tables sysschobjs sysrowsets syscolpars")
	showHeader := flag.Bool("header", false, "show page header")
	showPageStats := flag.Bool("showpagestats", false, "show page statistics")
	tableName := flag.String("table", "", "show table (use all for all tables)")
	showTableContent := flag.Bool("showcontent", false, "show table contents")
	showTableSchema := flag.Bool("showschema", true, "show table schema")
	showGamExtents := flag.Bool("gam", false, "show GAM extents for each page")
	showSGamExtents := flag.Bool("sgam", false, "show SGAM extents for each page")
	showIAMExtents := flag.Bool("iam", false, "show IAM extents for each page")
	showDataCols := flag.Bool("datacols", false, "show data cols for each data row")
	showSlots := flag.Bool("slots", false, "show page slots")
	showPFS := flag.Bool("pfs", false, "show pfm page allocation")
	showTableAllocation := flag.Bool("showTableAllocation", false, "show pages that the table has been allocated")
	userTable := flag.String("usertable", "", "get system table info about user table")
	export := flag.Bool("export", false, "export table")
	exportFormat := flag.String("format", "", "select format to export (csv)")

	flag.Parse()

	file, err := os.Open(*inputfile) //
	if err != nil {
		// handle the error here
		fmt.Printf("err %s for reading the mdf file ", err)
		return
	}

	fsize, err := file.Stat() //file descriptor
	if err != nil {
		return
	}
	// read the file

	defer file.Close()

	bs := make([]byte, PAGELEN) //byte array to hold one PAGE 8KB
	var database db.Database
	pages := page.PagesMap{}

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
		ShowPageStats:       *showPageStats}

	fmt.Println("Processing pages...")
	totalProcessedPages := 0
	for i := 0; i < int(fsize.Size()); i += PAGELEN {
		_, err := file.ReadAt(bs, int64(i))

		if err != nil {
			fmt.Printf("error reading file --->%s prev offset %d  mod %d",
				err, i/PAGELEN, i%PAGELEN)
			return
		}

		if *selectedPage != -1 && (i/PAGELEN < *selectedPage || i/PAGELEN > *selectedPage) && !*showPageStats {
			continue
		}

		if !*showPageStats && (i/PAGELEN) < *fromPage {
			continue
		}

		if !*showPageStats && *toPage != -1 && (i/PAGELEN) > *toPage {
			continue
		}
		//	fmt.Printf("Processing page  %d offset %d\n", i/8192, i)
		page := database.ProcessPage(bs)
		pages[page.Header.ObjectId] = append(pages[page.Header.ObjectId], page)

		totalProcessedPages++

	}

	if *pageType != "" {
		pages = pages.FilterByType(*pageType) //mutable

	}

	if *systemTables != "" {
		pages = pages.FilterBySystemTables(*systemTables)

	}

	if *userTable != "" {
		pages = pages.FilterBySystemTables("sysschobjs")
	}

	fmt.Printf("Processed %d pages.\n", totalProcessedPages)
	fmt.Println("Reconstructing tables...")
	database.PagesMap = pages
	database.Name = strings.Split(*inputfile, ".")[0]

	tables := database.GetTablesInformation(*tableName)
	database.Tables = tables

	fmt.Printf("Reconstructed %d tables.\n", len(tables))
	fmt.Println("Reporting & exporting stage.")

	reporter.ShowPageInfo(database, uint32(*selectedPage))
	reporter.ShowTableInfo(database)

	if *export {
		exp := exporter.Exporter{Format: *exportFormat}
		exp.Export(database)
	}

}
