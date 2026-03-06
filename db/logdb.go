package db

import (
	LDF "MSSQLParser/ldf"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"errors"
	"fmt"
	"os"
)

type LogDB struct {
	LogPage       page.Page
	VLFs          *LDF.VLFs
	LogRecordsMap LDF.RecordsMap
}

func (logdb *LogDB) ProcessLDF(lname string, carve bool) (int, error) {
	fmt.Printf("about to process database log file %s \n", lname)

	file, err := os.Open(lname)

	if err != nil {
		// handle the error here
		fmt.Printf("err %s reading the ldf file. \n", err)
	}
	defer file.Close()
	offset := 0

	bs := make([]byte, PAGELEN) //byte array to hold one PAGE 8KB
	_, err = file.ReadAt(bs, int64(offset))
	if err != nil {
		fmt.Printf("error reading log page at offset %d\n", offset)
		return 0, err
	}

	err = logdb.LogPage.Process(bs, offset, carve, 0)
	if err != nil {
		return 0, err
	}
	logdb.VLFs = new(LDF.VLFs)
	recordsProcessed := logdb.VLFs.Process(*file, carve)
	fmt.Printf("LDF processing completed %d log records processed\n", recordsProcessed)

	return recordsProcessed, nil
}

func (logdb LogDB) GetRecords() LDF.Records {
	var records LDF.Records
	for _, vlf := range *logdb.VLFs {

		for _, block := range vlf.Blocks {
			records = append(records, block.Records...)
		}
	}
	return records
}

func (logdb LogDB) ShowLDF(filterloptype string) {
	for _, vlf := range *logdb.VLFs {
		vlf.ShowInfo(filterloptype)
	}
}

func (logdb LogDB) WalkLSN(filterloptype string, direction string) {
	for _, vlf := range *logdb.VLFs {
		vlf.WalkLSN(direction, filterloptype)
	}
}

func (logdb LogDB) ShowPagesLDF(pagesId []uint32) {
	for _, pageId := range pagesId {
		fmt.Printf("PageID %d changes: \n", pageId)
		for _, vlf := range *logdb.VLFs {
			for _, block := range vlf.Blocks {

				filteredRecords := block.Records.FilterByPageID(pageId)
				for _, record := range filteredRecords {
					record.ShowLOPInfo("any")
				}
			}
		}

	}
}

func (logdb LogDB) GetBindingID() string {
	return utils.StringifyGUID(logdb.LogPage.FileHeader.BindingID[:])
}

func (logdb LogDB) GetMinLSN() (utils.LSN, error) {
	records := logdb.GetRecords()
	lop_end_records := records.FilterByOperation("LOP_END_CKPT")
	if lop_end_records == nil {
		return utils.LSN{}, errors.New("no LOP_END_CKPT found")
	} else {
		return lop_end_records[len(lop_end_records)-1].Lop_End_CKPT.MinLSN, nil
	}
}
