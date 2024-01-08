package LDF

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
	"os"
)

/*
hen a page is modified in the buffer cache, it isn't immediately written back to disk;
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
2: The slot number where the log record is located, in the log offset array*/

type VLFs []VLF

type VLF struct {
	Header *VLFHeader
	Blocks []LogBlock
}

type LogBlock struct {
	Header        *LogBlockHeader
	Records       []Record
	RecordOffsets RecordOffsets
}

/*VLF starts with a parity byte*/
/*72 bytes*/
type VLFHeader struct {
	Unknown1    [4]byte
	FSeqNo      uint32 //4-
	Unknown2    [8]byte
	FileSize    uint32 //16-
	Unknown3    [4]byte
	StartOffset uint32    //24- points to logblock header?
	CreateLSN   utils.LSN //32-
	Status      uint16    //?
}

// log block is integer multiple of 512 <60KB
// The first block always has a block offset that points past the first 8 KB in the VLF
type LogBlockHeader struct {
	Unknown1 [2]byte
	NofSlots uint16 //0-2number of live log records
	Size     uint16 //2-4 in-use area within from the beginnign of the log blocks to the end of array of record offsets
	Unknown2 [6]byte
	FirstLSN utils.LSN //
}

//stored in reversed order It consists of 2-byte values that represent the
//start position of each record. stored at the end of the allocated block

type RecordOffsets []uint16
type OriginalParityBytes []uint8

// he corresponding log records will contain the data page number and the slot number of the data page they affect.
// aligned at 4 byte boundary
// Every transaction must have an LOP_BEGIN_XACT
// and a record to close the xact, usually LOP_COMMIT_XACT.
type Record struct {
	Length        uint16    //size of fixed length area
	PreviousLSN   utils.LSN //
	Flag          uint16
	TransactionID uint64
	Operation     uint8 //what type of data is stored
	Context       uint8
}

func (record Record) GetOperationType() string {
	return OperationType[record.Operation]
}

func (record Record) GetContextType() string {
	return ContextType[record.Context]
}

func (vlfs VLFs) Process(file os.File) {

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
			fmt.Printf("error reading log page ---\n")
			return
		}
		vlfheader := new(VLFHeader)
		vlfheader.Process(bs)
		vlf.Header = vlfheader
		offset += int64(vlf.Header.StartOffset)

		for offset <= int64(vlf.Header.FileSize) {
			//log block header
			bs = make([]byte, 72)
			_, err = file.ReadAt(bs, offset)
			if err != nil {
				fmt.Printf("error reading log page ---\n")
				return
			}
			logBlock := new(LogBlock)
			logBlock.ProcessHeader(bs)
			if logBlock.Header.Size == 0 {
				msg := fmt.Sprintf("LogBlock header size is zero exiting processing vlf at offset %d", offset)
				mslogger.Mslogger.Warning(msg)
				break
			}
			bs = make([]byte, logBlock.Header.Size)
			_, err = file.ReadAt(bs, offset)
			if err != nil {
				fmt.Printf("error reading log page at --- %d\n", offset)
				return
			}
			logBlock.ProcessRecords(bs)

			vlf.Blocks = append(vlf.Blocks, *logBlock)
			offset += int64(logBlock.Header.Size)
		}
		vlfs = append(vlfs, *vlf)
		offset = int64(vlf.Header.FileSize) //needs check
	}

}

//LOP_INSERT_ROWS, LOP_DELETE_ROWS, and LOP_MODIFY_RO  jjjjjjjjjW
// LOP_BEGIN_XACT operations. This log record marks the beginning of a transaction
// the only log record that contains the date and time when the transaction started,
//user SID

func (logBlock *LogBlock) ProcessRecords(bs []byte) {
	recordOffsets := make(RecordOffsets, logBlock.Header.NofSlots)
	for recordId := 0; recordId < len(recordOffsets); recordId++ {
		recordOffsets[recordId] = utils.ToUint16(bs[len(bs)-2*(recordId+1) : len(bs)-2*recordId])
	}
	logBlock.Records = make([]Record, len(recordOffsets))

	for idx, recordOffset := range recordOffsets {
		record := new(Record)
		utils.Unmarshal(bs[recordOffset:], record)
		logBlock.Records[idx] = *record
	}

}

func (logBlock *LogBlock) ProcessHeader(bs []byte) {
	logBlockHeader := new(LogBlockHeader)
	logBlockHeader.Process(bs)
	logBlock.Header = logBlockHeader
}

func (logBlockHeader *LogBlockHeader) Process(bs []byte) {
	utils.Unmarshal(bs, logBlockHeader)
}

func (vlfheader *VLFHeader) Process(bs []byte) {
	utils.Unmarshal(bs, vlfheader)
}
