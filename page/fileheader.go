package page

import "MSSQLParser/utils"

type FileHeader struct {

	// ---- File header record starts at offset 0x0060 ----

	FileHeaderVersion uint32 // 0x0060
	FileSizePages     uint32 // 0x0064
	MaxSizePages      uint32 // 0x0068 (0 = unlimited)
	Growth            uint32 // 0x006C (pages or % depending on flags)
	FileID            uint32 // 0x0070 (1 for primary)
	FileGroupID       uint32 // 0x0074

	LogGroupGUID [16]byte // 0x0078 (rarely used)
	FileGUID     [16]byte // 0x0088
	DiffBaseGUID [16]byte // 0x0098

	DiffBaseLSN uint64 // 0x00A8
	BackupLSN   uint64 // 0x00B0

	// FileName is stored as Unicode (NVARCHAR), max 260 chars.
	// You’ll typically decode this manually from UTF-16LE.
	FileNameRaw [260 * 2]byte // 0x00B8

	// Optional secondary name / mirror; version-dependent.
	FileName2Raw [260 * 2]byte // 0x02B0

	Status        uint32   // 0x04A8 (bitmask)
	TDEFlag       uint32   // 0x04AC
	TDEThumbprint [16]byte // 0x04B0

	CreateLSN  uint64 // 0x04C0
	DropLSN    uint64 // 0x04C8
	BackupLSN2 uint64 // 0x04D0

}

func (fileHeader FileHeader) GetFileName() string {

	return utils.CleanUTF16LE(utils.DecodeUTF16(fileHeader.FileNameRaw[:]))

}
