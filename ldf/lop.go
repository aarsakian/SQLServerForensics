package LDF

import (
	datac "MSSQLParser/data"
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"errors"
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

type LOP_INSERT_DELETE struct {
	RowId                utils.RowId //0-8 locate the page
	Unknown              [4]byte     //8-12
	PreviousPageLSN      utils.LSN   //12-22
	Unknown2             [2]byte     //22-24
	PartitionID          uint64      //24-32 locate the table
	OffsetInRow          uint16      //starting position of the modified data within the data row
	ModifySize           uint16      //34-36
	RowFlags             [2]byte     //36-38
	NumElements          uint16      //38-40 num of varlen cols
	RowLogContentOffsets []uint16
	DataRow              *datac.DataRow
	RowLogContents       [][]byte // other rowlog contents except DataRow
}

type LOP_MODIFY struct {
	RowId                utils.RowId //0-8 locate the page
	Unknown              [4]byte     //8-12
	PreviousPageLSN      utils.LSN   //12-22
	Unknown2             [2]byte     //22-24
	PartitionID          uint64      //24-32 locate the table
	OffsetInRow          uint16      //starting position of the modified data within the data row
	ModifySize           uint16      //34-36
	RowFlags             [2]byte     //36-38
	NumElements          uint16      //38-40 num of varlen cols
	RowLogContentOffsets []uint16
	DataRow              *datac.DataRow
	RowLogContentBefore  []byte // other rowlog contents except DataRow
	RowLogContentAfter   []byte
}

/*other lop types that have RowID info*/
//62 bytes
type Generic_LOP struct {
	RowId           utils.RowId //0-8 locate the page
	Unknown         [4]byte     //8-12
	PreviousPageLSN utils.LSN   //12-22
	Unknown2        [2]byte     //22-24
	PartitionID     uint64      //24-32 locate the table
}

func (generic_lop *Generic_LOP) Process(bs []byte) {
	utils.Unmarshal(bs, generic_lop)
}

func (lop_begin_ckpt *LOP_BEGIN_CKPT) Process(bs []byte) {
	utils.Unmarshal(bs, lop_begin_ckpt)
}

func (lop_end_ckpt *LOP_END_CKPT) Process(bs []byte) {
	utils.Unmarshal(bs, lop_end_ckpt)
}

func (lop_insert_del_mod LOP_INSERT_DELETE) ShowInfo() {
	fmt.Printf("FileID:PageID:SlotID %s Prev Page LSN %s Partition ID %d\n",
		lop_insert_del_mod.RowId.ToStr(), lop_insert_del_mod.PreviousPageLSN.ToStr(),
		lop_insert_del_mod.PartitionID)
}

func (Lop_Insert_Delete *LOP_INSERT_DELETE) Process(bs []byte) {
	if 40 > len(bs) {
		return
	}
	offset, err := utils.Unmarshal(bs, Lop_Insert_Delete)
	if err != nil {
		return
	}

	mslogger.Mslogger.Info(fmt.Sprintf("processing lop insert/del rec row %d page %d",
		Lop_Insert_Delete.RowId.SlotNumber, Lop_Insert_Delete.RowId.PageId))

	Lop_Insert_Delete.ProcessRowContents(bs[offset:])

}

func (Lop_Insert_Delete *LOP_INSERT_DELETE) ProcessRowContents(bs []byte) {

	bsoffset := uint16(0)
	if Lop_Insert_Delete.NumElements*2%4 != 0 {
		bsoffset += (4 - Lop_Insert_Delete.NumElements*2%4)
	}

	for _, rowlogcontentoffset := range Lop_Insert_Delete.RowLogContentOffsets {

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
		//statusA || corruption statusB || row-compression
		if bs[bsoffset] == 0x30 || bs[bsoffset] == 0x10 || bs[bsoffset] == 0x70 {
			if int(bsoffset) > len(bs) || int(bsoffset)+int(rowlogcontentoffset) > len(bs) {
				mslogger.Mslogger.Info(fmt.Sprintf("exiting exceeded log block size by %d block size %d pageID",
					bsoffset, len(bs)))
				return
			}
			datarow := new(datac.DataRow)
			datarow.Parse(bs[bsoffset:int(bsoffset)+int(rowlogcontentoffset)], int(bsoffset)+int(rowlogcontentoffset), 0)

			Lop_Insert_Delete.DataRow = datarow
		} else {
			mslogger.Mslogger.Info("lop insert/delete but datarow not found")
		}

	}
}

func (lop_modify *LOP_MODIFY) Process(bs []byte) error {
	if 40 > len(bs) {
		return errors.New("not enough data to parse")
	}
	offset, err := utils.Unmarshal(bs, lop_modify)
	if err != nil {
		return err
	}

	mslogger.Mslogger.Info(fmt.Sprintf("processing lop insert/del rec row %d page %d",
		lop_modify.RowId.SlotNumber, lop_modify.RowId.PageId))

	oldValueLen := utils.ToUint16(bs[offset : offset+2])
	newValueLen := utils.ToUint16(bs[offset+2 : offset+4])

	if offset+4+int(oldValueLen) > len(bs) ||
		offset+4+int(oldValueLen)+int(newValueLen) > len(bs) {
		msg := fmt.Sprintf("exceeding available length in modify loop %d with modify size %d\n",
			offset+4+int(oldValueLen)-len(bs), lop_modify.ModifySize)
		mslogger.Mslogger.Info(msg)
		return errors.New(msg)
	}
	copy(lop_modify.RowLogContentBefore, bs[offset+4:offset+4+int(oldValueLen)])
	copy(lop_modify.RowLogContentAfter,
		bs[offset+4+int(oldValueLen):offset+4+int(oldValueLen)+int(newValueLen)])
	return nil
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

func (lop_end_cpkt LOP_END_CKPT) ShowInfo() {
	fmt.Printf("EndTime %s database end version %d\n",
		utils.DateTimeTostr(lop_end_cpkt.EndTime[:]), lop_end_cpkt.EndDBVers)
}

func (lop_begin_ckpt LOP_BEGIN_CKPT) ShowInfo() {
	fmt.Printf("BegTime %s database begin version %d\n",
		utils.DateTimeTostr(lop_begin_ckpt.BeginTime[:]), lop_begin_ckpt.BeginDBVersion)
}

func (lop_commit *LOP_COMMIT) Process(bs []byte) {
	utils.Unmarshal(bs, lop_commit)

}

func (lop_commit LOP_COMMIT) ShowInfo() {
	fmt.Printf("EndTime %s Trans Begin %s XactID %d\n", utils.DateTimeTostr(lop_commit.EndTime[:]),
		lop_commit.TransactionBegin.ToStr(), lop_commit.XactID)
}
