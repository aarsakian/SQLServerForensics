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
	RowLogContentOffsets []uint16
	DataRow              *page.DataRow
	RowLogContents       [][]byte // other rowlog contents except DataRow
}

/*other lop types that have RowID info*/
//62 bytes
type GENERIC_LOP struct {
	RowId           utils.RowId //0-8 locate the page
	Unknown         [4]byte     //8-12
	PreviousPageLSN utils.LSN   //12-22
	Unknown2        [2]byte     //22-24
	PartitionID     uint64      //24-32 locate the table
}

func (generic_lop *GENERIC_LOP) Process(bs []byte) {
	utils.Unmarshal(bs, generic_lop)
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
	if 40 > len(bs) {
		return
	}
	utils.Unmarshal(bs, lop_insert_delete_mod)

	mslogger.Mslogger.Info(fmt.Sprintf("processing lop insert/del rec row %d page %d",
		lop_insert_delete_mod.RowId.SlotNumber, lop_insert_delete_mod.RowId.PageId))

	lop_insert_delete_mod.ProcessRowContents(bs[40:])

}

func (lop_insert_delete_mod *LOP_INSERT_DELETE_MOD) ProcessRowContents(bs []byte) {

	bsoffset := 2 * lop_insert_delete_mod.NumElements
	if lop_insert_delete_mod.NumElements*2%4 != 0 {
		bsoffset += (4 - lop_insert_delete_mod.NumElements*2%4)
	}

	for _, rowlogcontentoffset := range lop_insert_delete_mod.RowLogContentOffsets {

		if rowlogcontentoffset == 0 { //exp to check
			bsoffset += 1 //move to next row log content
			continue
		}
		if int(bsoffset+rowlogcontentoffset) >= len(bs) ||
			int(rowlogcontentoffset) >= len(bs) {
			mslogger.Mslogger.Info(fmt.Sprintf("exceeded log block size by %d block size %d",
				bsoffset+rowlogcontentoffset, len(bs)))
			break
		}
		if bs[bsoffset] == 0x30 || bs[bsoffset] == 0x10 || bs[bsoffset] == 0x70 {
			if int(bsoffset) > len(bs) || int(bsoffset)+int(rowlogcontentoffset) > len(bs) {
				mslogger.Mslogger.Info(fmt.Sprintf("exiting exceeded log block size by %d block size %d pageID",
					bsoffset, len(bs)))
				return
			}
			datarow := new(page.DataRow)
			datarow.Parse(bs[bsoffset:bsoffset+rowlogcontentoffset], int(bsoffset)+int(rowlogcontentoffset), -1)

			lop_insert_delete_mod.DataRow = datarow
		} else {
			if int(bsoffset) > len(bs) || int(bsoffset)+int(rowlogcontentoffset) > len(bs) {
				return
			}
			rowlogcontents := make([]byte, len(bs[bsoffset:bsoffset+rowlogcontentoffset]))
			copy(rowlogcontents, bs[bsoffset:bsoffset+rowlogcontentoffset])
			lop_insert_delete_mod.RowLogContents = append(lop_insert_delete_mod.RowLogContents, rowlogcontents)
		}

		bsoffset += rowlogcontentoffset + 4 - (rowlogcontentoffset % 4)

	}
}

func (lop_begin *LOP_BEGIN) Process(bs []byte) {

	utils.Unmarshal(bs, lop_begin)
	if 60+int(lop_begin.TransactionNameLen) > len(bs) {
		return
	}
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
