package correlation

import (
	LDF "MSSQLParser/ldf"
	"MSSQLParser/utils"
	"time"
)

type CorrelatedRecord struct {
	LSN            utils.LSN
	TxID           string
	XactID         uint32
	Operation      string
	FileID         int
	PageID         int
	AllocationUnit uint64
	ObjectID       int
	Timestamp      time.Time
}

type CorrelationEngine struct {
	ByLSN      map[utils.LSN]*CorrelatedRecord
	ByTxID     map[string][]*CorrelatedRecord
	ByXactID   map[uint32][]*CorrelatedRecord
	ByPage     map[utils.PageSlot][]*CorrelatedRecord
	ByObjectID map[int][]*CorrelatedRecord
}

func (ce *CorrelationEngine) CorrelateRecords(records LDF.Records, allocToObjectID map[uint64]int32) {
	if ce.ByLSN == nil {
		ce.ByLSN = make(map[utils.LSN]*CorrelatedRecord)
	}
	if ce.ByTxID == nil {
		ce.ByTxID = make(map[string][]*CorrelatedRecord)
	}
	if ce.ByXactID == nil {
		ce.ByXactID = make(map[uint32][]*CorrelatedRecord)
	}
	if ce.ByPage == nil {
		ce.ByPage = make(map[utils.PageSlot][]*CorrelatedRecord)
	}
	if ce.ByObjectID == nil {
		ce.ByObjectID = make(map[int][]*CorrelatedRecord)
	}

	for _, record := range records {
		cr := &CorrelatedRecord{
			LSN:  record.CurrentLSN,
			TxID: record.TransactionID.ToStr(),

			Operation: record.GetOperationType(),
		}
		if record.Lop_Insert_Delete != nil {
			cr.FileID = int(record.Lop_Insert_Delete.RowId.FileId)
			cr.PageID = int(record.Lop_Insert_Delete.RowId.PageId)
			cr.AllocationUnit = record.Lop_Insert_Delete.PartitionID
		} else if record.Generic_LOP != nil {
			cr.FileID = int(record.Generic_LOP.RowId.FileId)
			cr.PageID = int(record.Generic_LOP.RowId.PageId)
			cr.AllocationUnit = record.Generic_LOP.PartitionID

		} else if record.Lop_Begin != nil {
			cr.Timestamp = utils.DateTimeToTime(record.Lop_Begin.BeginTime[:])
			cr.XactID = record.Lop_Begin.XactID
		} else if record.Lop_End_CKPT != nil {
			cr.Timestamp = utils.DateTimeToTime(record.Lop_End_CKPT.EndTime[:])
		} else if record.Lop_Commit != nil {
			cr.Timestamp = utils.DateTimeToTime(record.Lop_Commit.EndTime[:])
			cr.XactID = record.Lop_Commit.XactID
		}

		if cr.AllocationUnit != 0 {
			if objectID, ok := allocToObjectID[cr.AllocationUnit]; ok {
				cr.ObjectID = int(objectID)
			}
		}

		ce.ByLSN[record.CurrentLSN] = cr
		if cr.TxID != "0000:00000000" {
			ce.ByTxID[cr.TxID] = append(ce.ByTxID[cr.TxID], cr)
		}
		if cr.XactID != 0 {
			ce.ByXactID[cr.XactID] = append(ce.ByXactID[cr.XactID], cr)
		}
		if cr.FileID != 0 && cr.PageID != 0 {
			ce.ByPage[utils.PageSlot{FileId: uint16(cr.FileID), PageId: uint32(cr.PageID)}] =
				append(ce.ByPage[utils.PageSlot{FileId: uint16(cr.FileID), PageId: uint32(cr.PageID)}], cr)
		}
		if cr.ObjectID != 0 {
			ce.ByObjectID[cr.ObjectID] = append(ce.ByObjectID[cr.ObjectID], cr)
		}
	}
}
