package LDF

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
	"math"
	"os"
)

/*if a page is modified in the buffer cache, it isn't immediately written back to disk;
 instead, the page is marked as dirty
checkpoint writes the current in-memory modified pages (known as dirty pages)
 and transaction log information from memory to disk
Every data page keeps the LSN of the last log record that modified it.
every database caches log records in memory before flushing them to disk in batches of up to 60 KB.
hardened (been saved to the log file) log records
A write-ahead logging mechanism guarantees that dirty data pages are never saved into
the data files until the corresponding log records are hardened in the transaction log

Current LSN in a VLF marks the end of transaction logfile
MinLSN is the log sequence number of the oldest log record that is required for a successful database-wide rollback.
SIMPLE recovery model, the active part of transaction log starts with VLF, which contains
the oldest LSN of the oldest active transaction or the last CHECKPOINT
*/

/* LSN structure
4:The VLF number where the log record is located,
4: The starting block number of the log segment where the log record is located (i.e., the block
number of the above 512-byte size), with a length of 4 bytes, and the block numbers in each log
segment are numbered sequentially starting from 0. e.g. 0x001b2=434 => 8192+434*512
(starting physical offset)
2: The slot number where the log record is located, in the log offset array
in the SIMPLE recovery model, the active part of transaction log starts with VLF, which contains
the oldest LSN of the oldest active transaction
the last CHECKPOINT
the FULL or BULK LOGGED recovery models, the active part of transaction log starts with
VLF, which contains the oldest of the following:
LSN of the last log backup
LSN of the oldest active transaction
LSN of the process that reads transaction log records
*/

var LOGBLOCKMINSIZE int = 512

type VLFs []VLF

type VLF struct {
	Header *VLFHeader
	Blocks []LogBlock
}

type LogBlock struct {
	Header        *LogBlockHeader
	Records       Records
	RecordOffsets RecordOffsets
}

type RecordOffsets []uint16

/*VLF starts with a parity byte*/
/*72 bytes*/
type VLFHeader struct {
	Unknown1    [1]byte
	Parity      uint8 //2
	DatabaseID  uint16
	FSeqNo      uint32 //4- keeps track of the vlf order
	Unknown2    [8]byte
	FileSize    uint64    //16-24
	StartOffset uint64    //24- points to logblock header?
	CreateLSN   utils.LSN //32- value at the time the VLF was added to the transaction log.
	Status      uint8     //?

	FileId       uint8
	RecordUnitId uint8
}

// log block is integer multiple of 512 <60KB
// The first block always has a block offset that points past the first 8 KB in the VLF
type LogBlockHeader struct {
	FlagBit  uint8
	Unknown1 uint8
	NofSlots uint16 //2-4number of live log records
	Size     uint16 //4-6 in-use area within from the beginnign of the log blocks to the end of array of record offsets
	Unknown2 [6]byte
	FirstLSN utils.LSN // 12-
}

// The fifth bit indicates whether the block is the first block
func (logblockheader LogBlockHeader) IsFirst() bool {
	return logblockheader.FlagBit&0x10 == 0x10
}

// the fourth bit indicates whether it is the last block.
func (logblockheader LogBlockHeader) IsLast() bool {
	return logblockheader.FlagBit&0x08 == 0x08
}

func (logblockheadr LogBlockHeader) IsValid() bool {
	return logblockheadr.FlagBit == 0x40 || logblockheadr.FlagBit == 0x50 ||
		logblockheadr.FlagBit == 0x58
}

func (logBlock LogBlock) GetSize() int64 {
	nofBlocks := int64(math.Ceil(float64(logBlock.Header.Size) / float64(LOGBLOCKMINSIZE)))
	return nofBlocks * int64(LOGBLOCKMINSIZE)
}

func (vlfs *VLFs) Process(file os.File, carve bool, minLSN utils.LSN) int {
	recordsProcessed := 0

	offset := int64(8192)

	fi, err := file.Stat()
	if err != nil {
		fmt.Printf("Could not obtain stat, handle error")

	}

	for offset < fi.Size() {
		vlf := new(VLF)

		bs := make([]byte, 72)
		_, err := file.ReadAt(bs, offset)
		defer file.Close()

		if err != nil {
			fmt.Printf("error reading at %d \n", offset)
			return recordsProcessed
		}
		vlfheader := new(VLFHeader)
		vlfheader.Process(bs)
		vlf.Header = vlfheader
		mslogger.Mslogger.Info(fmt.Sprintf("Located VLF at %d", offset))
		logBlockoffset := int64(8192)

		for logBlockoffset <= int64(vlf.Header.FileSize) {
			//log block header
			bs = make([]byte, 72)
			_, err = file.ReadAt(bs, offset+logBlockoffset)
			if err != nil {
				fmt.Printf("error reading log while parsing logblock at %d\n", offset+logBlockoffset)
				return recordsProcessed
			}
			logBlock := new(LogBlock)
			logBlock.ProcessHeader(bs)

			if logBlock.Header.Size == 0 {
				msg := fmt.Sprintf("LogBlock header size is zero at offset %d continuing", offset+logBlockoffset)
				mslogger.Mslogger.Warning(msg)
				logBlockoffset += int64(LOGBLOCKMINSIZE)
				continue
			} else if !logBlock.Header.IsValid() {
				msg := fmt.Sprintf("LogBlock header invalid at offset %d continuing", offset+logBlockoffset)
				mslogger.Mslogger.Warning(msg)
				logBlockoffset += int64(LOGBLOCKMINSIZE)
				continue
			}
			mslogger.Mslogger.Info(fmt.Sprintf("Located log block at %d Nof Slots %d",
				offset+logBlockoffset, logBlock.Header.NofSlots))
			//logBlock size
			bs = make([]byte, logBlock.Header.Size)
			_, err = file.ReadAt(bs, offset+logBlockoffset)
			if err != nil {
				fmt.Printf("error reading log page at --- %d\n", offset)
				return recordsProcessed
			}
			recordsProcessed += logBlock.ProcessRecords(bs, offset+logBlockoffset, carve, minLSN)

			vlf.Blocks = append(vlf.Blocks, *logBlock)

			logBlockoffset += logBlock.GetSize()

		}
		*vlfs = append(*vlfs, *vlf)

		if int64(vlf.Header.FileSize) == 0 {
			msg := fmt.Sprintf("VLF header size is zero exiting processing vlfs at offset %d", offset)
			mslogger.Mslogger.Warning(msg)
			break
		}
		offset += int64(vlf.Header.FileSize) //needs check
	}
	return recordsProcessed

}

//LOP_INSERT_ROWS, LOP_DELETE_ROWS, and LOP_MODIFY_RO
// LOP_BEGIN_XACT operations. This log record marks the beginning of a transaction
// the only log record that contains the date and time when the transaction started,
//user SID

func (logBlock *LogBlock) ProcessRecords(bs []byte, baseOffset int64, carve bool, minLSN utils.LSN) int {
	recordOffsets := make(RecordOffsets, logBlock.Header.NofSlots)

	LSN_to_Record := make(map[utils.LSN]*Record)

	for recordId := 0; recordId < len(recordOffsets); recordId++ {
		if len(bs) < 2*(recordId+1) {
			msg := fmt.Sprintf("log record id exceeds buffer at %d", recordId)
			mslogger.Mslogger.Warning(msg)
			break
		}
		recordOffsets[recordId] = utils.ToUint16(bs[len(bs)-2*(recordId+1) : len(bs)-2*recordId])

	}

	currentLSN := logBlock.Header.FirstLSN

	for _, recordOffset := range recordOffsets {
		if len(bs) < int(recordOffset) {
			msg := fmt.Sprintf("log record offset exceeds buffer at %d", recordOffset)
			mslogger.Mslogger.Warning(msg)
			break
		}
		record := new(Record)
		utils.Unmarshal(bs[recordOffset:], record)
		LSN_to_Record[currentLSN] = record
		if currentLSN.IsGreater(minLSN) {
			record.Carved = false
		} else {
			record.Carved = true
		}
		record.CurrentLSN = currentLSN

		prevRecord := LSN_to_Record[record.PreviousLSN]
		if prevRecord != nil {
			record.PreviousRecord = prevRecord
			prevRecord.NextRecord = record
		}

		currentLSN.Increment()

		//omit carved records
		if !carve && record.Carved {
			continue
		}

		//LOP_BEGIN_CKPT = start of checkpoint
		//LOP_END_CKPT = end of checkpoint
		switch OperationType[record.Operation] {

		case "LOP_INSERT_ROW", "LOP_DELETE_ROW", "LOP_MODIFY_ROW", "LOP_INSERT_SPLIT",
			"LOP_DELETE_SPLIT", "LOP_MODIFY_SPLIT":

			Lop_Insert_Delete_Mod_mod := new(LOP_INSERT_DELETE_MOD)
			Lop_Insert_Delete_Mod_mod.Process(bs[recordOffset+24:])
			record.Lop_Insert_Delete_Mod = Lop_Insert_Delete_Mod_mod

		case "LOP_BEGIN_XACT":
			lop_begin_xact := new(LOP_BEGIN)
			lop_begin_xact.Process(bs[recordOffset+24:])
			record.Lop_Begin = lop_begin_xact
		case "LOP_COMMIT_XACT":
			lop_commit := new(LOP_COMMIT)
			lop_commit.Process(bs[recordOffset+24:])
			record.Lop_Commit = lop_commit
		case "LOP_BEGIN_CKPT":
			lop_begin_ckpt := new(LOP_BEGIN_CKPT)
			lop_begin_ckpt.Process(bs[recordOffset+24:])
			record.Lop_Begin_CKPT = lop_begin_ckpt
		case "LOP_END_CKPT":
			lop_end_ckpt := new(LOP_END_CKPT)
			lop_end_ckpt.Process(bs[recordOffset+24:])
			record.Lop_End_CKPT = lop_end_ckpt
		case "LOP_EXPUNGE_ROWS":
			lop_exp_row := new(Generic_LOP)
			lop_exp_row.Process(bs[recordOffset+24:])
			record.Generic_LOP = lop_exp_row
		}

		logBlock.Records = append(logBlock.Records, *record)

		mslogger.Mslogger.Info(fmt.Sprintf("Record %s at %d", record.GetOperationType(),
			int64(recordOffset)+baseOffset))

	}
	return len(logBlock.Records)
}

func (logBlock *LogBlock) ProcessHeader(bs []byte) {
	logBlockHeader := new(LogBlockHeader)
	logBlockHeader.Process(bs)
	logBlock.Header = logBlockHeader
}

func (logBlockHeader *LogBlockHeader) Process(bs []byte) {
	utils.Unmarshal(bs, logBlockHeader)
}

func (logBlockHeader LogBlockHeader) FirstToStr() string {
	return logBlockHeader.FirstLSN.ToStr()
}

func (logBlock LogBlock) ShowInfo(filterloptype string) {
	if filterloptype == "" {
		fmt.Printf("Log Block Header Slots %d size %d FirstLSN %s\n",
			logBlock.Header.NofSlots, logBlock.Header.Size, logBlock.Header.FirstToStr())
	}

	for _, record := range logBlock.Records {
		record.ShowLOPInfo(filterloptype)
	}
}

func (vlfheader *VLFHeader) Process(bs []byte) {
	utils.Unmarshal(bs, vlfheader)
}

func (vlfheader VLFHeader) CreateToStr() string {
	return vlfheader.CreateLSN.ToStr()
}

func (vlf VLF) ShowInfo(filterloptype string) {
	fmt.Printf("VLF Header\n")
	fmt.Printf("database_id %d begin_offset %d size %d sequence number %d parity %d create_lsn %s\n",
		vlf.Header.DatabaseID, vlf.Header.StartOffset,
		vlf.Header.FileSize, vlf.Header.FSeqNo, vlf.Header.Parity, vlf.Header.CreateToStr())
	for _, logblock := range vlf.Blocks {
		logblock.ShowInfo(filterloptype)
	}
}
