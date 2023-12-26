package ldf

import "MSSQLParser/utils"

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

/*VLF starts with a parity byte*/
/*72 bytes*/
type VLFHeader struct {
	FSeqNo      uint32
	FileSize    uint32 //24-
	StartOffset uint32
	CreateLSN   utils.LSN
}

//log block is integer multiple of 512 <60KB
//The first block always has a block offset that points past the first 8 KB in the VLF
type LOGBlockHeader struct {
	nofSlots int       //number of live log records
	size     int       //in-use area within from the beginnign of the log blocks to the end of array of record offsets
	firstLSN utils.LSN //
}

//stored in reversed order It consists of 2-byte values that represent the
//start position of each record.

type RecordOffsets []uint16
type OriginalParityBytes []uint8

//aligned at 4 byte boundary
type Record struct {
	length        uint16    //size of fixed lenght area
	previousLSN   utils.LSN //
	flag          uint16
	transactionID uint64
	operation     uint8 //what type of data is stored
	context       uint8
}
