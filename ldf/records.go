package LDF

import (
	"MSSQLParser/utils"
	"errors"
	"fmt"
	"slices"
	"time"
)

//stored in reversed order It consists of 2-byte values that represent the
//start position of each record. stored at the end of the allocated block

type OriginalParityBytes []uint8

type Records []*Record

type RecordsMap map[utils.LSN]*Record

// 4 byte prefix
type RecordPrefix struct {
	Flags      uint8
	SlotNumber uint8
	Size       uint16
}

// he corresponding log records will contain the data page number and the slot number of the data page they affect.
// aligned at 4 byte boundary
// Every transaction must have an LOP_BEGIN_XACT
// and a record to close the xact, usually LOP_COMMIT_XACT.
type Record struct {
	PreviousLSN       utils.LSN           //0-10
	TransactionID     utils.TransactionID //10-16
	Flag              uint16              //16-18
	Operation         uint8               //what type of data is stored 19
	Context           uint8               //20
	Lop_Insert_Delete *LOP_INSERT_DELETE
	Lop_Begin         *LOP_BEGIN
	Lop_Commit        *LOP_COMMIT
	Lop_Begin_CKPT    *LOP_BEGIN_CKPT
	Lop_End_CKPT      *LOP_END_CKPT
	Generic_LOP       *Generic_LOP
	PreviousRecord    *Record
	NextRecord        *Record
	Carved            bool
	IsActive          bool
	Prefix            *RecordPrefix
	CurrentLSN        utils.LSN //this value will be set by context
}

type ByIncreasingLSN []*Record

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
	return OperationsType[record.Operation]
}

func (record Record) GetContextType() string {
	return ContextType[record.Context]
}

func (record Record) HasGreaterEqualLSN(lsn utils.LSN) bool {
	return record.CurrentLSN.IsGreaterEqual(lsn)
}

func (record Record) HasLessEqualLSN(lsn utils.LSN) bool {
	return !record.CurrentLSN.IsGreaterEqual(lsn)
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
		fmt.Printf("Current LSN %s Previous LSN %s TransID %s Flag Bits %d %s %s Active %t\n",
			record.CurrentLSN.ToStr(),
			record.PreviousLSN.ToStr(), record.TransactionID.ToStr(),
			record.Flag,
			OperationsType[record.Operation],
			record.GetContextType(), record.IsActive)
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
	} else if record.Lop_Begin_CKPT != nil &&
		(filterloptype == "begin_ckpt" || filterloptype == "any") {
		record.Lop_Begin_CKPT.ShowInfo()
	} else if record.Lop_End_CKPT != nil &&
		(filterloptype == "end_ckpt" || filterloptype == "any") {
		record.Lop_End_CKPT.ShowInfo()
	}
}

func (record Record) HasOperationType(operationtypes []string) bool {
	return slices.Contains(operationtypes, record.GetOperationType())
}

func (record Record) WalkInfo(direction string, loptype string) {

	switch direction {

	case "backward":
		record.WalkInfoBackwards(loptype)
	case "forward":
		record.WalkInfoForward(loptype)
	case "any":
		record.WalkInfoBackwards(loptype)
		record.WalkInfoForward(loptype)

	}
}

func (record Record) WalkInfoBackwards(loptype string) {
	for record.PreviousRecord != nil {
		fmt.Printf(" <-  \t")
		record.PreviousRecord.ShowLOPInfo(loptype)
		record = *record.PreviousRecord
	}
}

func (record *Record) UpdateActiveStatus(minLSN utils.LSN) {
	if record.HasGreaterEqualLSN(minLSN) {
		record.IsActive = true
	} else { // only when asked for carve
		record.IsActive = true
	}
}

func (record Record) WalkInfoForward(loptype string) {
	for record.NextRecord != nil {
		fmt.Printf(" ->  \t")
		record.NextRecord.ShowLOPInfo(loptype)
		record = *record.NextRecord
	}
}

func (record Record) HasPageID(pageID uint32) bool {
	return record.Lop_Insert_Delete != nil &&
		record.Lop_Insert_Delete.RowId.PageId == pageID ||
		record.Generic_LOP != nil &&
			record.Generic_LOP.RowId.PageId == pageID
}

func (records Records) FilterByOperation(operationType string) Records {
	return utils.Filter(records, func(record *Record) bool {
		return record.GetOperationType() == operationType
	})
}

func (records Records) FilterByGreaterLSN(lsn utils.LSN) Records {
	return utils.Filter(records, func(record *Record) bool {
		return record.HasGreaterEqualLSN(lsn)
	})
}

func (records Records) FilterByLessLSN(lsn utils.LSN) Records {
	return utils.Filter(records, func(record *Record) bool {
		return record.HasLessEqualLSN(lsn)
	})
}

func (records Records) FilterByOperations(operationtypes []string) Records {
	return utils.Filter(records, func(record *Record) bool {
		return record.HasOperationType(operationtypes)
	})
}

func (records RecordsMap) FilterOutNullOperations() Records {
	return utils.FilterMapToList(records, func(record *Record) bool {
		return record.Operation != 0
	})
}

func (records Records) FilterByPageID(pageID uint32) Records {
	return utils.Filter(records, func(record *Record) bool {
		return record.HasPageID(pageID)
	})

}

func (records Records) GroupByPageID() map[uint32]Records {
	grouped := make(map[uint32]Records)

	for _, record := range records {
		if record.Lop_Insert_Delete != nil {
			pageID := record.Lop_Insert_Delete.RowId.PageId
			grouped[pageID] = append(grouped[pageID], record)
		} else if record.Generic_LOP != nil {
			pageID := record.Generic_LOP.RowId.PageId
			grouped[pageID] = append(grouped[pageID], record)
		}
	}
	return grouped
}

func (records Records) GroupByTransactionID() map[string]Records {
	grouped := make(map[string]Records)
	for _, record := range records {
		txID := record.TransactionID.ToStr()
		grouped[txID] = append(grouped[txID], record)
	}
	return grouped
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

func (records Records) DetermineLastActiveTransactionLSN() (utils.LSN, error) {
	oldestActiveLSN := utils.LSN{}
	activeRecords := make(map[string]utils.LSN)
	lop_begin_records := records.FilterByOperation("LOP_BEGIN_XACT")
	lop_commit_xact_records := records.FilterByOperation("LOP_COMMIT_XACT")
	lop_abort_xact_records := records.FilterByOperation("LOP_ABORT_XACT")

	for _, record := range lop_begin_records {
		activeRecords[record.TransactionID.ToStr()] = record.CurrentLSN
	}
	for _, record := range lop_commit_xact_records {
		delete(activeRecords, record.TransactionID.ToStr())
	}
	for _, record := range lop_abort_xact_records {
		delete(activeRecords, record.TransactionID.ToStr())
	}

	if len(activeRecords) == 0 {
		return oldestActiveLSN, fmt.Errorf("no active transactions found")
	} else {
		for _, lsn := range activeRecords {
			oldestActiveLSN = lsn
			break
		}
	}
	return oldestActiveLSN, nil
}

func (records Records) UpdateActiveStatus(minLSN utils.LSN) {
	for idx := range records {
		records[idx].UpdateActiveStatus(minLSN)

	}
}

func (records Records) LocateStoredMinLSN() (utils.LSN, error) {
	lop_end_records := records.FilterByOperation("LOP_END_CKPT")
	if lop_end_records == nil {
		return utils.LSN{}, errors.New("no LOP_END_CKPT found")
	} else {
		return lop_end_records[len(lop_end_records)-1].Lop_End_CKPT.MinLSN, nil
	}
}

func (records Records) DetermineBeginLSNOfCheckpoint() (utils.LSN, error) {
	lop_begin_ckpt_records := records.FilterByOperation("LOP_BEGIN_CKPT")
	if len(lop_begin_ckpt_records) == 0 {
		return utils.LSN{}, errors.New("no LOP_BEGIN_CKPT found")
	}

	currentLSN := lop_begin_ckpt_records[0].CurrentLSN

	for _, record := range lop_begin_ckpt_records[1:] {
		if record.CurrentLSN.IsGreaterEqual(currentLSN) {

			currentLSN = record.CurrentLSN
		}
	}

	return currentLSN, nil
}

func (records Records) LocateDirtyPages(lastBeginLSN utils.LSN) map[uint32]utils.LSN {
	var dirtyPagesMap = make(map[uint32]utils.LSN)

	for _, record := range records {
		if record.CurrentLSN.IsGreaterEqual(lastBeginLSN) {

			if record.Lop_Insert_Delete != nil {
				lastLSN, ok := dirtyPagesMap[record.Lop_Insert_Delete.RowId.PageId]
				if ok {
					if record.CurrentLSN.IsLess(lastLSN) {
						dirtyPagesMap[record.Lop_Insert_Delete.RowId.PageId] = record.CurrentLSN
					}
				} else {
					dirtyPagesMap[record.Lop_Insert_Delete.RowId.PageId] = record.CurrentLSN
				}

			} else if record.Generic_LOP != nil {
				lastLSN, ok := dirtyPagesMap[record.Generic_LOP.RowId.PageId]
				if ok {
					if record.CurrentLSN.IsLess(lastLSN) {
						dirtyPagesMap[record.Generic_LOP.RowId.PageId] = record.CurrentLSN
					}
				} else {
					dirtyPagesMap[record.Generic_LOP.RowId.PageId] = record.CurrentLSN
				}
			}

		}
	}
	return dirtyPagesMap
}
