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
	"MSSQLParser/page"
	"flag"
	"fmt"
	"os"
)

type Database page.Pages

func (db Database) ShowStats() {
	for _, page := range db {
		allocMap := page.GetAllocationMaps()
		if allocMap == nil {
			continue
		}
		allocMap.ShowAllocations()
	}

}

func main() {

	PAGELEN := 8192

	inputfile := flag.String("db", "", "absolute path to the MDF file")
	selectedPage := flag.Int("page", -1, "select a page to start parsing")
	fromPage := flag.Int("from", 0, "select page id to start parsing")
	toPage := flag.Int("to", -1, "select page id to end parsing")

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
	var database Database

	for i := 0; i < int(fsize.Size()); i += PAGELEN {
		_, err := file.ReadAt(bs, int64(i))

		if err != nil {
			fmt.Printf("error reading file --->%s prev offset %d  mod %d",
			 err, i/PAGELEN, i%PAGELEN)
			return
		}

		if *selectedPage != -1 && (i/PAGELEN < *selectedPage || i/PAGELEN > *selectedPage) {
			continue
		}
		
		if (i/PAGELEN) < *fromPage {
			continue
		}

		if *toPage != -1 && (i/PAGELEN)> *toPage {
			continue
		}

		var page *page.Page = new(page.Page)
		page.Process(bs)
		database = append(database, *page)
		if page.Header.PageId != 0 {
			fmt.Printf("Processed page %d type %s\n", page.Header.PageId, page.GetType())
		}
	

	}
	database.ShowStats()

}
