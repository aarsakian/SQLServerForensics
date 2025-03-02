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
	Lop_Insert_Delete *LOP_INSERT_DELETE_MOD
	Lop_Begin         *LOP_BEGIN
	Lop_Commit        *LOP_COMMIT
	Lop_Begin_CKPT    *LOP_BEGIN_CKPT
	Lop_End_CKPT      *LOP_END_CKPT
	PreviousRecord    *Record
	NextRecord        *Record
	Carved            bool
}

type ByDecreasingLSN []Record

func (b ByDecreasingLSN) Less(i, j int) bool {

	return b[i].CurrentLSN.IsGreater(b[j].CurrentLSN)
}

func (b ByDecreasingLSN) Swap(i, j int) {

	b[i], b[j] = b[j], b[i]
}

func (b ByDecreasingLSN) Len() int {
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
			ContextType[record.Context])
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
		record.Lop_Insert_Delete.RowId.PageId == pageID
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
