package db

import (
	LDF "MSSQLParser/ldf"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"fmt"
	"os"
)

type LogDB struct {
	LogPage      page.Page
	VLFs         *LDF.VLFs
	LSNToRecords LDF.RecordsMap
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

func (logdb LogDB) GetDbiCheckptLSN() (utils.LSN, error) {
	records := logdb.GetRecords()
	beginLSN, err2 := records.DetermineBeginLSNOfCheckpoint()
	if err2 != nil {
		return utils.LSN{}, err2
	} else {
		return beginLSN, nil
	}

}

func (logdb LogDB) GetMinLSN(beginLSN utils.LSN) (utils.LSN, error) {
	records := logdb.GetRecords()
	oldestActiveLSN, err1 := records.DetermineLastActiveTransactionLSN()

	if err1 != nil {
		return beginLSN, err1
	} else {
		if oldestActiveLSN.IsGreaterEqual(beginLSN) {
			return beginLSN, nil
		} else {
			return oldestActiveLSN, nil
		}
	}

}

func (logdb LogDB) LocateDirtyPages(lastBeginLSN utils.LSN) map[uint32]utils.LSN {

	records := logdb.GetRecords()
	return records.LocateDirtyPages(lastBeginLSN)

}
