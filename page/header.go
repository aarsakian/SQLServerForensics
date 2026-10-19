package page

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
	"strings"
)

// SQL Server Page Header FlagBits (bitmask)
const (
	FlagIsPacked          = 0x0001 //  packed data row
	FlagImmediateRollback = 0x0002 //
	FlagHasGhostRecords   = 0x0004 //
	FlagIsModified        = 0x0008 // Dirty or tracks an active transactional modification
	FlagBStat             = 0x0010 // row offset tables or slot allocations match active tracking
	FlagBulkLogged        = 0x0020 // Bulk methods used
	FlagHasTornBits       = 0x0100 // Torn page detection
	FlagHasChecksum       = 0x0200 // Checksum used
	FlagIsEncrypted       = 0x0400 //
	FlagMirrored          = 0x0800 // Ghost version records present
	FlagIAM               = 0x8000
)

// Human-readable descriptions
var flagDescriptions = map[uint16]string{
	FlagIsModified:        "PAGE_IS_PACKED",
	FlagImmediateRollback: "PAGE_IMMEDIATE_ROLLBACK",
	/*	FlagIsMixedExtent:           "PAGE_IS_MIXED_EXTENT",
		FlagHasDifferentOwner:       "PAGE_HAS_DIFFERENT_OWNER",
		FlagGhostCleanupPending:     "PAGE_IS_GHOST_CLEANUP_PENDING",*/
	FlagHasChecksum: "PAGE_HAS_CHECKSUM",
	FlagHasTornBits: "PAGE_HAS_TORN_BITS",
	FlagIsEncrypted: "PAGE_IS_ENCRYPTED",
	FlagIAM:         "PAGE_IS_IAM_PAGED",
	/*	FlagHasDifferentialChange:   "PAGE_HAS_DIFFERENTIAL_CHANGE",
		FlagHasFullLogging:          "PAGE_HAS_FULL_LOGGING",
		FlagHasSparseMapping:        "PAGE_HAS_SPARSE_MAPPING",
		FlagIsVersionStore:          "PAGE_IS_VERSION_STORE",
		FlagHasGhostVersionRecords:  "PAGE_HAS_GHOST_VERSION_RECORDS",
		FlagIsCompressionDictionary: "PAGE_IS_COMPRESSION_DICTIONARY",
		FlagHasInRowLOBPointers:     "PAGE_HAS_IN_ROW_LOB_POINTERS",
		FlagIsFileStreamData:        "PAGE_IS_FILESTREAM_DATA",
		FlagIsTempdbMetadata:        "PAGE_IS_TEMPDB_METADATA",*/
}

// DecodeFlagBits returns a list of active flags for a given bitmask.
func (header Header) DecodeFlagBits() string {
	var result strings.Builder
	for flag, desc := range flagDescriptions {
		if flag == header.FlagBits {
			result.WriteString(desc)
			result.WriteString(",")
		}
	}
	return result.String()
}

type Header struct {
	Version               uint8     //1
	Type                  uint8     // 1-2
	TypeFlagBits          uint8     //2-3
	Level                 uint8     // 0 = leaf
	FlagBits              uint16    //4-6
	IndexId               uint16    //6-8  0 = Heap 1 = Clustered Index  (AllocUnitId.idInd))
	PrevPage              uint32    //8-12
	PreviousFileId        uint16    //12-14
	PMinLen               uint16    //14-16  size of fixed len records
	NextPage              uint32    //16-20
	NextPageFileId        uint16    //20-22
	SlotCnt               uint16    //22-24   number of slots (records) that hold data
	ObjectId              uint32    //24-28 AllocUnitId.idObj
	FreeCnt               uint16    //28-30 free space in bytes
	FreeData              uint16    //30-32 offset from the start of the page to the first byte after the last record
	PageId                uint32    //32-36
	FragId                uint32    //36-40
	LSN                   utils.LSN //40-50  LSN of the last log record that affected the page.
	XactReserved          uint16    //50-52 	Number of bytes reserved by the most recently started transaction
	XdeslDPart2           uint32    //52-54
	XdeslIDPart1          uint16    //54-58
	GhostRecCnt           uint16    //58-60
	TornBits              int32     //60-64 bit string 1 bit -> sector
	Checksum              uint32    //checksum for SQL Server 2005+
	Reserved              [28]byte  //68-96
	TornPatternConsistent bool
}

func (header Header) isTornPageUsed() bool {
	return header.FlagBits&FlagHasTornBits == FlagHasTornBits
}

func (header *Header) setTornPatternConsistent(data []byte) {
	//sector 1 omit 1st sector
	referencedPattern := byte(data[511] & 0x03)
	for sector := 2; sector < int(PAGELEN-512)/512; sector++ {
		//two torn bit for each sector
		//skip first sector

		pattern := byte(data[512*(sector+1)-1] & 0x03)
		if referencedPattern != pattern {
			header.TornPatternConsistent = false
			break
		}

	}
	header.TornPatternConsistent = true
}

func (header Header) isIAMPaged() bool {
	return header.FlagBits&FlagIAM == FlagIAM
}

func (header Header) isChecksumUsed() bool {
	return header.FlagBits&FlagHasChecksum == FlagHasChecksum
}

func (header Header) isValid() bool {

	for typeId := range PageTypes {
		if typeId == header.Type {
			return true
		}
	}
	mslogger.Mslogger.Warning(fmt.Sprintf("Page Id %d Unknown page type %d", header.PageId, header.Type))
	return false
}

func (header Header) GetMetadataAllocUnitId() uint64 {
	return uint64(header.IndexId)<<48 | uint64(header.ObjectId)<<16
}

func (header Header) sanityCheck() bool {
	if header.Version != 1 {

		mslogger.Mslogger.Warning(fmt.Sprintf("Invalid page header version %d", header.Version))
		return false
	}
	if header.LSN.IsZeroed() {
		mslogger.Mslogger.Warning(fmt.Sprintf("LSN is zero at pageId %d", header.PageId))
		return false
	}

	if header.IndexId >= 251 && header.IndexId < 255 ||
		header.IndexId > 271 {
		mslogger.Mslogger.Warning(fmt.Sprintf("Invalid Index Id %d at pageId %d",
			header.IndexId, header.PageId))
	}

	if header.SlotCnt > 4048 { //8192-96/2
		mslogger.Mslogger.Warning(fmt.Sprintf("number of slots exceeded maximum allowed number %d.", header.SlotCnt))
		return false
	}
	if header.FreeData > 8192-2 && header.Type == 1 { // exclude header size at least one slot
		mslogger.Mslogger.Warning(fmt.Sprintf("Data Page free area exceeded max allowed size %d", header.FreeData))
		return false

	} else if header.FreeData > 8186 && header.Type == 10 {
		mslogger.Mslogger.Warning(fmt.Sprintf("IAM Page free area exceeded max allowed size %d", header.FreeData))
		return false
	}

	return true
}

func (header Header) Print() {
	if header.TornBits != 0 {

		fmt.Printf("Page Id %d type %s objectid %d LSN %s index %d flags %s slots %d free space %d Prev page %d  Next page %d TornBits Pattern Consistent %t\n",
			header.PageId, PageTypes[header.Type], header.ObjectId, header.LSN.ToStr(),
			header.IndexId, header.DecodeFlagBits(),
			header.SlotCnt, header.FreeData, header.PrevPage, header.NextPage, header.TornPatternConsistent)
	} else {
		fmt.Printf("Page Id %d type %s objectid %d LSN %s index %d flags %s slots %d free space %d Prev page %d  Next page %d \n",
			header.PageId, PageTypes[header.Type], header.ObjectId, header.LSN.ToStr(),
			header.IndexId, header.DecodeFlagBits(),
			header.SlotCnt, header.FreeData, header.PrevPage, header.NextPage)
	}

}
