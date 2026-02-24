package page

import (
	datac "MSSQLParser/data"
	"MSSQLParser/utils"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
)

type FileHeader struct {

	// ---- File header record starts at offset 0x0060 ----

	BindingID                    [16]byte // 0x008a 138
	FileIDProp                   uint16   // 154
	FileID                       uint16   //
	Size                         uint32
	MaxSize                      uint32
	Growth                       uint32
	Perf                         uint32
	BackupLSN                    utils.LSN
	FirstUpdateLSN               utils.LSN
	OldestRestoreLSN             utils.LSN
	FirstNonloggedUpdateLsn      utils.LSN
	MinSize                      uint32
	Status                       uint32
	UserShrinkSize               uint32 //d4
	SectorSize                   uint32
	MaxLSN                       utils.LSN
	FirstLSN                     utils.LSN
	CreateLSN                    utils.LSN
	DifferentialBaseLsn          utils.LSN
	DifferentialBaseGuid         [16]byte
	FileOfflineLsn               utils.LSN
	FileIdGuid                   [16]byte
	RestoreStatus                uint32
	RestoreRedoStartLsn          utils.LSN
	FileName                     []byte //database Name
	RestoreSourceGuid            [16]byte
	MaxLsnBranchId               [16]byte
	SecondaryRedoStartLsn        utils.LSN
	SecondaryDifferentialBaseLsn utils.LSN
	ReadOnlyLsn                  utils.LSN
	ReadWriteLsn                 utils.LSN
	RestoreDifferentialBaseLsn   utils.LSN
	RestoreDifferentialBaseGuid  [16]byte
	/*RestorePathOrigin = [NULL]          DatabaseEncryptionFileState = [NULL]
	FCBFileDEK = [NULL]                 ProxyFileIdProp = [Error accessing Column]
	ForeignRedoLsn = [NULL]             ForeignRedoTime = [NULL]            ForeignRedoOldestBeginLsn = [NULL]
	*/

}

func (fileHeader *FileHeader) Parse(datarow datac.DataRow) error {

	structValPtr := reflect.ValueOf(fileHeader)
	structType := reflect.TypeOf(fileHeader)

	if structType.Elem().Kind() != reflect.Struct {
		return errors.New("must be a struct")
	}
	for i := 0; i < structValPtr.Elem().NumField(); i++ {
		field := structValPtr.Elem().Field(i) //StructField type
		if i >= len(*datarow.VarLenCols) {
			return fmt.Errorf("exceed available number of var len cols at Field %s idx %d ", field, i)
		}
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
		case reflect.Slice:

			var dst []byte

			dst = make([]byte, len(val))
			copy(dst, val)

			field.Set(reflect.ValueOf(dst))

		}

	}
	return nil
}

func (fileHeader FileHeader) GetFileName() string {

	return utils.CleanUTF16LE(utils.DecodeUTF16(fileHeader.FileName[:]))

}

func (fileHeader FileHeader) ShowInfo() {
	fmt.Printf("NofPages %d FileName %s MaxLsnBranchId %s\n", fileHeader.Size, fileHeader.GetFileName(),
		utils.StringifyGUID(fileHeader.MaxLsnBranchId[:]))
}
