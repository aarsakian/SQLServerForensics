package LDF

import (
	"MSSQLParser/utils"
	"fmt"
)

//stored in reversed order It consists of 2-byte values that represent the
//start position of each record. stored at the end of the allocated block

type OriginalParityBytes []uint8

// he corresponding log records will contain the data page number and the slot number of the data page they affect.
// aligned at 4 byte boundary
// Every transaction must have an LOP_BEGIN_XACT
// and a record to close the xact, usually LOP_COMMIT_XACT.
type Record struct {
	Unknown           [2]byte
	Length            uint16              //size of fixed length area 2-4
	PreviousLSN       utils.LSN           //4-14
	Flag              uint16              //14-16
	TransactionID     utils.TransactionID //16-22
	Operation         uint8               //what type of data is stored 23
	Context           uint8               //24
	Lop_Insert_Delete *LOP_INSERT_DELETE_MOD
	Lop_Begin         *LOP_BEGIN
	Lop_Commit        *LOP_COMMIT
}

func (record Record) GetOperationType() string {
	return OperationType[record.Operation]
}

func (record Record) GetContextType() string {
	return ContextType[record.Context]
}

func (record Record) ShowLOPInfo(filterloptype string) {
	if filterloptype == "any" {
		fmt.Printf("PreviousLSN %s transactionID %s %s %s \n",
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
