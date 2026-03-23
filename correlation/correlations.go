package correlation

import (
	LDF "MSSQLParser/ldf"
	"MSSQLParser/utils"
	"time"
)

type CorrelatedRecord struct {
	LSN       utils.LSN
	TxID      string
	XactID    uint32
	Operation string

	PartitionId uint64
	ObjectID    int32
	Timestamp   time.Time
	RowId       *utils.RowId
	LogRecord   *LDF.Record
}

type CorrelationEngine struct {
	ByLSN      map[utils.LSN]*CorrelatedRecord
	ByTxID     map[string][]*CorrelatedRecord
	ByXactID   map[uint32][]*CorrelatedRecord
	ByPage     map[uint32][]*CorrelatedRecord
	ByObjectID map[int32][]*CorrelatedRecord
}

func (ce *CorrelationEngine) CorrelateRecords(records LDF.Records, partitionIdToObjectId map[uint64]int32) {
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
		ce.ByPage = make(map[uint32][]*CorrelatedRecord)
	}
	if ce.ByObjectID == nil {
		ce.ByObjectID = make(map[int32][]*CorrelatedRecord)
	}

	for _, record := range records {
		cr := &CorrelatedRecord{
			LSN:       record.CurrentLSN,
			TxID:      record.TransactionID.ToStr(),
			LogRecord: record,
			Operation: record.GetOperationType(),
		}
		if record.Lop_Insert_Delete != nil {
			cr.RowId = new(utils.RowId)
			//use zero based numbering log file
			cr.RowId.FileId = record.Lop_Insert_Delete.RowId.FileId - 1
			cr.RowId.PageId = record.Lop_Insert_Delete.RowId.PageId

			cr.PartitionId = record.Lop_Insert_Delete.PartitionID
			cr.ObjectID = partitionIdToObjectId[cr.PartitionId]
		} else if record.Generic_LOP != nil {
			cr.RowId = new(utils.RowId)
			cr.RowId.FileId = record.Generic_LOP.RowId.FileId - 1
			cr.RowId.PageId = record.Generic_LOP.RowId.PageId

			cr.PartitionId = record.Generic_LOP.PartitionID
			cr.ObjectID = partitionIdToObjectId[cr.PartitionId]
		} else if record.Lop_Modify != nil {
			cr.RowId = new(utils.RowId)
			cr.RowId.FileId = record.Lop_Modify.RowId.FileId - 1
			cr.RowId.PageId = record.Lop_Modify.RowId.PageId

			cr.PartitionId = record.Lop_Modify.PartitionID
			cr.ObjectID = partitionIdToObjectId[cr.PartitionId]
		} else if record.Lop_Begin != nil {
			cr.Timestamp = utils.DateTimeToTime(record.Lop_Begin.BeginTime[:])
			cr.XactID = record.Lop_Begin.XactID
		} else if record.Lop_End_CKPT != nil {
			cr.Timestamp = utils.DateTimeToTime(record.Lop_End_CKPT.EndTime[:])
		} else if record.Lop_Commit != nil {
			cr.Timestamp = utils.DateTimeToTime(record.Lop_Commit.EndTime[:])
			cr.XactID = record.Lop_Commit.XactID
		}

		ce.ByLSN[record.CurrentLSN] = cr
		if cr.TxID != "0000:00000000" {
			ce.ByTxID[cr.TxID] = append(ce.ByTxID[cr.TxID], cr)
		}
		if cr.XactID != 0 {
			ce.ByXactID[cr.XactID] = append(ce.ByXactID[cr.XactID], cr)
		}
		if cr.RowId != nil && cr.RowId.PageId != 0 {
			ce.ByPage[cr.RowId.PageId] =
				append(ce.ByPage[cr.RowId.PageId], cr)
		}
		if cr.ObjectID != 0 {
			ce.ByObjectID[cr.ObjectID] = append(ce.ByObjectID[cr.ObjectID], cr)
		}
	}
}

func (ce *CorrelationEngine) CorrelateTable(objectit int32) []*CorrelatedRecord {

	return ce.ByObjectID[objectit]
}
