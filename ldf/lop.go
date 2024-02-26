package LDF

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"fmt"
)

type LOP_BEGIN struct {
	SPID                      uint16
	Unknown                   [2]byte
	BeginlogStatus            uint32
	Unknown2                  [8]byte
	BeginTime                 [8]byte //DateTime
	XactID                    uint32
	Unknown3                  [10]byte
	OldestActiveTransactionID utils.TransactionID
	Unknown4                  [10]byte
	TransactionNameLen        uint16
	Unknown5                  [4]byte
	TransactionName           string
	TransactionSID            uint32
}

type LOP_COMMIT struct {
	EndTime          [8]byte //DateTime
	TransactionBegin utils.LSN
	Unknown          [22]byte
	XactID           uint32
}

type LOP_BEGIN_CKPT struct {
	BeginTime      [8]byte
	Unknown        [10]byte
	BeginDBVersion uint16
	MaxDESID       utils.TransactionID
}

type LOP_END_CKPT struct {
	EndTime   [8]byte
	MinLSN    utils.LSN
	EndDBVers uint16
}

type LOP_INSERT_DELETE_MOD struct {
	RowId                utils.RowId //0-8 locate the page
	Unknown              [4]byte     //8-12
	PreviousPageLSN      utils.LSN   //12-22
	Unknown2             [2]byte     //22-24
	PartitionID          uint64      //24-32 locate the table
	OffsetInRow          uint16      //starting position of the modified data within the data row
	ModifySize           uint16      //34-36
	RowFlags             [2]byte     //36-38
	NumElements          uint16      //38-40
	RowLogContentOffsets []int16
	DataRows             page.DataRows
}

func (lop_begin_ckpt *LOP_BEGIN_CKPT) Process(bs []byte) {
	utils.Unmarshal(bs, lop_begin_ckpt)
}

func (lop_end_ckpt *LOP_END_CKPT) Process(bs []byte) {
	utils.Unmarshal(bs, lop_end_ckpt)
}

func (lop_insert_del_mod LOP_INSERT_DELETE_MOD) ShowInfo() {
	fmt.Printf("FileID:PageID:SlotID %s Prev Page LSN %s Partition ID %d\n",
		lop_insert_del_mod.RowId.ToStr(), lop_insert_del_mod.PreviousPageLSN.ToStr(),
		lop_insert_del_mod.PartitionID)
}

func (lop_insert_delete_mod *LOP_INSERT_DELETE_MOD) Process(bs []byte) {

	utils.Unmarshal(bs, lop_insert_delete_mod)

	bsoffset := 40 + 2*lop_insert_delete_mod.NumElements
	for _, rowlogcontentoffset := range lop_insert_delete_mod.RowLogContentOffsets {
		datarow := new(page.DataRow)
		if rowlogcontentoffset == 0 { //exp to check
			bsoffset += 1
			continue
		}
		if int(bsoffset+uint16(rowlogcontentoffset)) >= len(bs) {
			mslogger.Mslogger.Info(fmt.Sprintf("exceeded log block size by %d block size %d",
				bsoffset+uint16(rowlogcontentoffset), len(bs)))
			break
		}
		utils.Unmarshal(bs[bsoffset:bsoffset+uint16(rowlogcontentoffset)], datarow)
		lop_insert_delete_mod.DataRows = append(lop_insert_delete_mod.DataRows, *datarow)

		bsoffset += uint16(rowlogcontentoffset)

	}
}

func (lop_begin *LOP_BEGIN) Process(bs []byte) {
	utils.Unmarshal(bs, lop_begin)
	lop_begin.TransactionName = utils.DecodeUTF16(bs[60 : 60+lop_begin.TransactionNameLen])
}

func (lop_begin *LOP_BEGIN) ShowInfo() {
	fmt.Printf("BegTime %s Xact ID %d Trans Name %s Trans SID %d\n",
		utils.DateTimeTostr(lop_begin.BeginTime[:]),
		lop_begin.XactID, lop_begin.TransactionName, lop_begin.TransactionSID)
}

func (lop_commit *LOP_COMMIT) Process(bs []byte) {
	utils.Unmarshal(bs, lop_commit)

}

func (lop_commit LOP_COMMIT) ShowInfo() {
	fmt.Printf("EndTime %s Trans Begin %s XactID %d\n", utils.DateTimeTostr(lop_commit.EndTime[:]),
		lop_commit.TransactionBegin.ToStr(), lop_commit.XactID)
}
