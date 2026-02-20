package page

import (
	datac "MSSQLParser/data"
	"MSSQLParser/utils"
	"encoding/binary"
	"errors"
	"reflect"
)

type FileHeader struct {

	// ---- File header record starts at offset 0x0060 ----

	BindingID                   [16]byte // 0x008a 138
	FileIDProp                  uint16   // 154
	FileID                      uint16   //
	Size                        uint32
	MaxSize                     uint32
	Growth                      uint32
	Perf                        uint32
	BackupLSN                   utils.LSN
	FirstUpdateLSN              utils.LSN
	OldestRestoreLSN            utils.LSN
	FirstNonloggedUpdateLsn     utils.LSN
	MinSize                     uint32
	Status                      uint32
	UserShrinkSize              uint32 //d4
	SectorSize                  uint32
	MaxLSN                      utils.LSN
	FirstLSN                    utils.LSN
	CreateLSN                   utils.LSN
	DifferentialBaseLsn         utils.LSN
	DifferentialBaseGuid        [16]byte
	FileOfflineLsn              utils.LSN
	FileIdGuid                  [16]byte
	RestoreStatus               uint32
	RestoreRedoStartLsn         utils.LSN
	RestoreSourceGuid           [16]byte
	MaxLsnBranchId              [16]byte
	ReadOnlyLsn                 utils.LSN
	ReadWriteLsn                utils.LSN
	RestoreDifferentialBaseLsn  utils.LSN
	RestoreDifferentialBaseGuid [16]byte

	/*FileGroupID uint32 //

	LogGroupGUID [16]byte // 0x0078 (rarely used)
	FileGUID     [16]byte // 0x0088
	DiffBaseGUID [16]byte // 0x0098

	Unknown              [4]byte
	ResotoreRedoStartLSN utils.LSN

	UknownLSN   utils.LSN
	DiffBaseLSN uint64 // 0x00A8

	// FileName is stored as Unicode (NVARCHAR), max 260 chars.
	// You’ll typically decode this manually from UTF-16LE.
	FileNameRaw [260 * 2]byte // 0x00B8

	// Optional secondary name / mirror; version-dependent.
	FileName2Raw [260 * 2]byte // 0x02B0

	TDEFlag       uint32   // 0x04AC
	TDEThumbprint [16]byte // 0x04B0

	DropLSN    uint64 // 0x04C8
	BackupLSN2 uint64 // 0x04D0*/

}

func (fileHeader *FileHeader) Parse(datarow datac.DataRow) error {

	structValPtr := reflect.ValueOf(fileHeader)
	structType := reflect.TypeOf(fileHeader)

	if structType.Elem().Kind() != reflect.Struct {
		return errors.New("must be a struct")
	}
	for i := 0; i < structValPtr.Elem().NumField(); i++ {
		field := structValPtr.Elem().Field(i) //StructField type
		val := (*datarow.VarLenCols)[i].Content

		switch field.Kind() {
		case reflect.Uint16:
			field.SetUint(uint64(binary.LittleEndian.Uint16(val)))
		case reflect.Uint32:
			field.SetUint(uint64(binary.LittleEndian.Uint32(val)))
		case reflect.Uint64:
			field.SetUint(uint64(binary.LittleEndian.Uint64(val)))
		case reflect.Struct:
			lsn := new(utils.LSN)

			utils.Unmarshal(val, lsn)
			field.Set(reflect.ValueOf(*lsn))
		case reflect.Array:

			arrT := reflect.ArrayOf(field.Len(), reflect.TypeFor[byte]()) //create array type to hold the slice
			arr := reflect.New(arrT).Elem()                               //initialize and access array
			n := field.Len()

			dst := arr.Slice(0, n).Bytes()
			copy(dst, val)
			field.Set(arr)

		}
	}
	return nil
}

func (fileHeader FileHeader) GetFileName() string {
	return ""
	//return utils.CleanUTF16LE(utils.DecodeUTF16(fileHeader.FileNameRaw[:]))

}
