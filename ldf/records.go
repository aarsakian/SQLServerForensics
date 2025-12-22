package LDF

import (
	"MSSQLParser/utils"
	"errors"
	"fmt"
	"time"
)

//stored in reversed order It consists of 2-byte values that represent the
//start position of each record. stored at the end of the allocated block

type OriginalParityBytes []uint8

type Records []Record

// he corresponding log records will contain the data page number and the slot number of the data page they affect.
// aligned at 4 byte boundary
// Every transaction must have an LOP_BEGIN_XACT
// and a record to close the xact, usually LOP_COMMIT_XACT.
type Record struct {
	CurrentLSN        utils.LSN
	Unknown           [2]byte
	Length            uint16              //size of fixed length area 2-4
	PreviousLSN       utils.LSN           //4-14 VLF:LOG BLOCK:LOG RECORD
	Flag              uint16              //14-16
	TransactionID     utils.TransactionID //16-22
	Operation         uint8               //what type of data is stored 23
	Context           uint8               //24
	Lop_Insert_Delete *LOP_INSERT_DELETE
	Lop_Begin         *LOP_BEGIN
	Lop_Commit        *LOP_COMMIT
	Lop_Begin_CKPT    *LOP_BEGIN_CKPT
	Lop_End_CKPT      *LOP_END_CKPT
	Generic_LOP       *Generic_LOP
	PreviousRecord    *Record
	NextRecord        *Record
	Carved            bool
}

type ByIncreasingLSN []Record

func (b ByIncreasingLSN) Less(i, j int) bool {

	return b[i].CurrentLSN.IsLess(b[j].CurrentLSN)
}

func (b ByIncreasingLSN) Swap(i, j int) {

	b[i], b[j] = b[j], b[i]
}

func (b ByIncreasingLSN) Len() int {
	return len(b)

}

func (record Record) GetOperationType() string {
	return OperationType[record.Operation]
}

func (record Record) GetContextType() string {
	return ContextType[record.Context]
}

func (record Record) HasGreaterLSN(lsn utils.LSN) bool {
	return record.PreviousLSN.IsGreater(lsn)
}

func (record Record) HasLessLSN(lsn utils.LSN) bool {
	return !record.PreviousLSN.IsGreater(lsn)
}

func (record Record) GetBeginRecordPtr() (*Record, error) {
	prevRecord := record.PreviousRecord
	for prevRecord != nil {
		if prevRecord.Lop_Begin != nil && prevRecord.TransactionID == record.TransactionID {
			return prevRecord, nil

		}
		prevRecord = prevRecord.PreviousRecord
	}
	return nil, errors.New("begin record not found")
}

func (record Record) GetBeginCommitDate() string {
	beginRecord, err := record.GetBeginRecordPtr()
	if err == nil {
		return utils.DateTimeTostr(beginRecord.Lop_Begin.BeginTime[:])
	} else {
		return "NA"
	}

}

func (record Record) GetBeginCommitDateObj() time.Time {
	beginRecord, err := record.GetBeginRecordPtr()
	if err == nil {
		return utils.DateTimeToObj(beginRecord.Lop_Begin.BeginTime[:])
	} else {
		return time.Time{}
	}

}

func (record Record) GetEndCommitDate() string {

	beginRecord, err := record.GetBeginRecordPtr()
	if err != nil {
		return "NA"
	}

	for beginRecord != nil {

		if beginRecord.Lop_Commit != nil && beginRecord.TransactionID == record.TransactionID {
			return utils.DateTimeTostr(beginRecord.Lop_Commit.EndTime[:])

		}
		beginRecord = beginRecord.NextRecord
	}

	return "NA"

}

func (record Record) ShowLOPInfo(filterloptype string) {
	if filterloptype == "any" {
		fmt.Printf("Current LSN %s Previous LSN %s transID %s %s %s \n",
			record.CurrentLSN.ToStr(),
			record.PreviousLSN.ToStr(), record.TransactionID.ToStr(),
			OperationType[record.Operation],
			record.GetContextType())
	}

	if record.Lop_Insert_Delete != nil &&
		(filterloptype == "insert" || filterloptype == "any") {
		record.Lop_Insert_Delete.ShowInfo()
	} else if record.Lop_Begin != nil &&
		(filterloptype == "begin" || filterloptype == "any") {
		record.Lop_Begin.ShowInfo()
	} else if record.Lop_Commit != nil &&
		(filterloptype == "commit" || filterloptype == "any") {
		record.Lop_Commit.ShowInfo()
	}

}

func (record Record) HasOperationType(operationtypes []string) bool {
	for _, operationtype := range operationtypes {
		if record.GetOperationType() == operationtype {
			return true
		}
	}
	return false
}
func (record Record) HasPageID(pageID uint32) bool {
	return record.Lop_Insert_Delete != nil &&
		record.Lop_Insert_Delete.RowId.PageId == pageID ||
		record.Generic_LOP != nil &&
			record.Generic_LOP.RowId.PageId == pageID
}

func (records Records) FilterByOperation(operationType string) Records {
	return utils.Filter(records, func(record Record) bool {
		return record.GetOperationType() == operationType
	})
}

func (records Records) FilterByGreaterLSN(lsn utils.LSN) Records {
	return utils.Filter(records, func(record Record) bool {
		return record.HasGreaterLSN(lsn)
	})
}

func (records Records) FilterByLessLSN(lsn utils.LSN) Records {
	return utils.Filter(records, func(record Record) bool {
		return record.HasLessLSN(lsn)
	})
}

func (records Records) FilterByOperations(operationtypes []string) Records {
	return utils.Filter(records, func(record Record) bool {
		return record.HasOperationType(operationtypes)
	})
}

func (records Records) FilterOutNullOperations() Records {
	return utils.Filter(records, func(record Record) bool {
		return record.Operation != 0
	})
}

func (records Records) FilterByPageID(pageID uint32) Records {
	return utils.Filter(records, func(record Record) bool {
		return record.HasPageID(pageID)
	})

}

func (records Records) HasExpungeOperation(askedIdx int) bool {
	for idx := range records {
		if idx < askedIdx {
			continue
		}
		if records[idx].GetOperationType() == "LOP_EXPUNGE_ROWS" {
			return true
		}
	}

	return false
}

func (records Records) DetermineMinLSN() utils.LSN {
	//locating latest LOP_END_CKPT lop
	lop_end_records := records.FilterByOperation("LOP_END_CKPT")
	latestDate := utils.DateTimeToObj(lop_end_records[0].Lop_End_CKPT.EndTime[:])
	recordId := 0
	for idx, record := range lop_end_records {
		if idx == 0 {
			continue
		}
		//get date
		newDate := utils.DateTimeToObj(record.Lop_End_CKPT.EndTime[:])
		if newDate.After(latestDate) {
			recordId = idx
			latestDate = newDate
		}
	}
	return lop_end_records[recordId].Lop_End_CKPT.MinLSN
}

func (records Records) UpdateCarveStatus(minLSN utils.LSN, carve bool) {
	for idx := range records {
		if records[idx].HasLessLSN(minLSN) {
			records[idx].Carved = true

		} else if carve { // only when asked for carve
			records[idx].Carved = false
		}
	}
}
