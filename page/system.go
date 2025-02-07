package page

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/utils"
	"fmt"
	"strings"
)

var TableType = map[string]string{"AF": "Aggregate function (CLR)", "U": "User Table", "S": "System Table",
	"V": "View", "P": "Stored Procedure", "TT": "Table Type", "UQ": "Unique Constraint", "C": "Check constraint",
	"F": "Foreign Key", "FS": "Assembly (CLR) Scalar function", "FN": "Scalar Function", "FT": "Assembly (CLR) Table-Valued function"}

/*a set of pages of one particular type for one particular
partition is called an allocation unit, so the final catalog view you need to learn about is sys.allocation_
units. Therefore, a partiotion can have more than one allocation unit */

type SysObjects struct { //view
	Name             []byte
	Id               uint32
	Xtype            [2]byte
	Uid              uint8
	Info             uint8
	Status           uint32
	Base_schema_ver  uint32
	Replinfo         uint32
	Parent_obj       uint32
	Crdate           [8]byte
	Ftcatid          uint8
	Schema_ver       uint32
	Stats_schema_ver uint32
	Type             [2]byte
	Userstat         uint8
	Sysstat          uint8
	IndexDel         uint8
	Refdate          [8]byte
	Version          uint32
	Deltring         uint32
	Instring         uint32
	Updtring         uint32
	Seltring         uint32
	Category         uint32
	Cache            uint8
}

type SysAllocUnits struct {
	Auid       [8]byte //0-8
	Type       uint8   //9
	OwnerId    uint64  //9-17
	Status     int32   //17-21
	Fgid       uint16  //21-23
	PgFirst    [6]byte //6 bytes
	Pgroot     [6]byte //6 bytes
	PgFirstIAM [6]byte //6
	PcUsed     uint64
	PcData     uint64
	PcReserved uint64
}

// syscolpars a contains a row for every column in a table (column definitions)
type SysColpars struct {
	Id          uint32 //4 -8  //objectID
	Number      uint16 //8-10
	Colid       uint16 //10 -12
	Unknown     [2]byte
	Xtype       uint8  // 14 sys.sysscalartypes.xtype.
	Utype       uint32 //15-19 sys.sysscalartypes.id
	Length      int16  //19-21
	Prec        uint8
	Scale       uint8
	Collationid uint32
	Status      uint32
	Maxinrow    uint16
	Xmlns       uint32
	Dflt        uint32
	Chk         uint32
	Idtval      []byte
	Name        []byte
}

// a row for every index or statistics
type SysIdxStats struct {
	Id        uint32 //objectID
	Indid     uint32
	Name      []byte
	Status    uint32
	Intprop   uint32
	Fillfact  uint8
	Type      uint8
	Tinyprop  uint8
	Dataspace uint32
	Lobds     uint32
	Rowsetid  uint64
}

type sysIsCols struct {
	Idmajor   uint32
	Idminor   uint32
	Subid     uint32
	Status    uint32
	Intprop   uint32
	Tinyprop1 uint8
	Tinyprop2 uint8
	Tinyprop3 uint8
	Tinyprop4 uint8
}

type SysRsCols struct {
	Rsid        uint64 //1-8 partition id
	Rscolid     int32  //8-12 column id
	Hbcolid     int32  //12 - 16  ordinal position of the column in the clustered index
	Rcmodified  int64  //16 -24
	Ti          int32  //24 -28
	Cid         uint32 //28 -32
	Ordkey      int16  //32 - 34
	Maxinrowlen int16  //34 - 36
	Status      int32  //36 - 40
	Offset      int32  //end offset of static column within datarow 40 - 44
	Nullbit     int32
	Bitpos      int16
	Olguid      []byte
}

type SysRowSets struct {
	Rowsetid   uint64
	Ownertype  uint8
	Idmajor    int32
	Idminor    uint32 // index id
	Numpart    uint32
	Status     uint32
	Fgidfs     uint16
	Rcrows     uint64
	Comprlevel uint8
	Fillfact   uint8
	Maxnullbit uint16
	Maxleaf    uint32
	Maxint     uint16
	Minint     uint16
	Rsguid     [16]byte
	Lockres    [8]byte
	Scope_id   uint32
}

// stores table/ contstraints information
type Sysschobjs struct {
	Id       int32   //0-3
	Nsid     uint32  //3-7
	Nsclass  uint8   //7-8
	Status   uint32  //8-12
	Type     [2]byte //12-14 bytes
	Pid      uint32  //14-18
	Pclas    uint8   //18-19
	Intprop  uint32  //19-23
	Created  [8]byte //23-31
	Modified [8]byte //31-39
	Status2  uint32  //39-43
	Name     []byte
}

type Result[F, S, T, FH, FT, SX, SV any] struct {
	First   F
	Second  S
	Third   T
	Fourth  FH
	Fifth   FT
	Sixth   SX
	Seventh SV
}

type SystemTable interface {
	GetName() string
	SetName([]byte)
	ShowData()
	GetData() (any, any)
}

func (sysobject SysObjects) GetData() (any, any) {
	return nil, nil
}

func (sysobject SysObjects) GetName() string {
	return ""
}

func (sysobject *SysObjects) SetName([]byte) {

}

func (sysobjects SysObjects) ShowData() {
	fmt.Printf("sysobjects")
}

func (sysidxstats SysIdxStats) GetData() (any, any) {
	return sysidxstats.Type, nil
}

func (sysidxstats SysIdxStats) GetName() string {
	return utils.DecodeUTF16(sysidxstats.Name)
}

func (sysidxstats *SysIdxStats) SetName(name []byte) {
	sysidxstats.Name = name
}

func (sysidxstats SysIdxStats) ShowData() {
	fmt.Printf("Idx Name %s", utils.DecodeUTF16(sysidxstats.Name))
}

func (sysrscols SysRsCols) GetName() string {
	return ""
}

func (sysrscols SysRsCols) GetData() (any, any) {
	return uint64(sysrscols.Rsid), Result[int32, int16, int64, int32, int32, int16, int32]{sysrscols.Rscolid,
		sysrscols.GetLeafOffset(), sysrscols.Rcmodified,
		sysrscols.Hbcolid, sysrscols.Rscolid, sysrscols.Ordkey, sysrscols.Nullbit}
}

func (sysrscols SysRsCols) GetLeafOffset() int16 {
	return int16(sysrscols.Offset & 0xffff)

}

func (sysrscols *SysRsCols) SetName([]byte) {

}

func (sysrscols *SysRsCols) ShowData() {
	fmt.Printf("sysrsobjects partition id %d  colid %d offset %d", sysrscols.Rsid, sysrscols.Cid, sysrscols.Offset)
}

func (sysrowsets *SysRowSets) SetName([]byte) {

}

func (sysrowsets SysRowSets) GetName() string {
	return ""
}

func (sysrowsets SysRowSets) ShowData() {
	fmt.Printf("sysrowsets %d %d \n", sysrowsets.Rowsetid, sysrowsets.Status)
}

func (sysrowsets SysRowSets) GetData() (any, any) {
	return int32(sysrowsets.Idmajor), Result[uint64, uint32, uint8, uint16, uint16, uint16, uint32]{
		sysrowsets.Rowsetid, sysrowsets.Idminor, sysrowsets.Comprlevel,
		sysrowsets.Maxint, sysrowsets.Minint, sysrowsets.Fgidfs, sysrowsets.Maxleaf}
	// table object ID, partition ID
}

func (sysiscols *sysIsCols) SetName([]byte) {

}

func (sysiscols sysIsCols) GetName() string {
	return ""
}

func (sysiscols sysIsCols) GetData() (any, any) {
	return 0, ""
}

func (sysiscols sysIsCols) ShowData() {
	fmt.Printf("sysiscols %d %d \n", sysiscols.Idmajor, sysiscols.Idminor)
}

func (sysallocunits *SysAllocUnits) SetName([]byte) {

}

func (sysallocunits SysAllocUnits) GetName() string {
	return ""
}

func (sysallocunits SysAllocUnits) GetData() (any, any) {
	return sysallocunits.OwnerId, utils.ToUint64(sysallocunits.Auid[:]) //partition id, allocunitid
}

func (sysallocunits SysAllocUnits) getIndexId() int {
	return int(utils.ToInt64(sysallocunits.Auid[:])) >> 48
}

func (sysallocunits SysAllocUnits) getPageId() int {
	indexId := sysallocunits.getIndexId()
	return int(utils.ToInt64(sysallocunits.Auid[:])) - (indexId<<48)>>16
}
func (sysallocunits SysAllocUnits) ShowData() {

	pageId := sysallocunits.getPageId()
	fmt.Printf("sysalloc Page ObjectId %d AllocUID %d %d  PartitionId  %d  %x %x %d\n",
		pageId, utils.ToInt64(sysallocunits.Auid[:]),
		sysallocunits.Type, sysallocunits.OwnerId,
		sysallocunits.PgFirst, sysallocunits.PgFirstIAM, sysallocunits.PcData)
}

func (syscolpars SysColpars) GetName() string {
	return utils.DecodeUTF16(syscolpars.Name)

}

func (syscolpars SysColpars) GetData() (any, any) {
	return int32(syscolpars.Id),
		Result[string, string, int16, uint16, uint32, uint8, uint8]{syscolpars.GetName(),
			syscolpars.GetType(), syscolpars.Length, syscolpars.Colid, syscolpars.Collationid, syscolpars.Prec, syscolpars.Scale}

}

func (syscolpars *SysColpars) SetName(name []byte) {
	syscolpars.Name = name
}

func (syscolpars SysColpars) GetType() string {
	if syscolpars.Xtype == 0x22 {
		return "image"
	} else if syscolpars.Xtype == 0x23 {
		return "text"
	} else if syscolpars.Xtype == 0x24 {
		return "uniqueidentifier"
	} else if syscolpars.Xtype == 0x28 {
		return "date"
	} else if syscolpars.Xtype == 0x20 {
		return "time"
	} else if syscolpars.Xtype == 0x2A {
		return "datetime2"
	} else if syscolpars.Xtype == 0x2B {
		return "datetimeoffset"
	} else if syscolpars.Xtype == 0x30 {
		return "tinyint"
	} else if syscolpars.Xtype == 0x34 {
		return "smallint"
	} else if syscolpars.Xtype == 0x38 {
		return "int"
	} else if syscolpars.Xtype == 0x3A {
		return "smalldatetime"
	} else if syscolpars.Xtype == 0x3B {
		return "real"
	} else if syscolpars.Xtype == 0x3c {
		return "money"
	} else if syscolpars.Xtype == 0x3D {
		return "datetime"
	} else if syscolpars.Xtype == 0x3E {
		return "float"
	} else if syscolpars.Xtype == 0x62 {
		return "sql_variant"
	} else if syscolpars.Xtype == 0x63 {
		return "ntext"
	} else if syscolpars.Xtype == 0x68 {
		return "bit"
	} else if syscolpars.Xtype == 0x6A {
		return "decimal"
	} else if syscolpars.Xtype == 0x06 {
		return "numeric"
	} else if syscolpars.Xtype == 0x7A {
		return "smallmoney"
	} else if syscolpars.Xtype == 0x7f {
		return "bigint"
	} else if syscolpars.Xtype == 0xA5 {
		return "varbinary"
	} else if syscolpars.Xtype == 0xA7 {
		return "varchar"
	} else if syscolpars.Xtype == 0xAD {
		return "binary"
	} else if syscolpars.Xtype == 0xAF {
		return "char"
	} else if syscolpars.Xtype == 0xBD {
		return "timestamp"
	} else if syscolpars.Xtype == 0xE7 {
		return "nvarchar"
	} else if syscolpars.Xtype == 0xEF {
		return "nchar"
	} else if syscolpars.Xtype == 0xF1 {
		return "xml"
	} else {
		msg := fmt.Sprintf("Type Not found 0x%x ", syscolpars.Xtype)
		mslogger.Mslogger.Warning(msg)
		return msg
	}

}

func (syscolpars SysColpars) ShowData() {
	fmt.Printf("syscolpars id %d  len %d  number %d Colorder %d %s \n",
		syscolpars.Id, syscolpars.Length, syscolpars.Number, syscolpars.Colid,
		syscolpars.GetName())
}

func (sysschobjs *Sysschobjs) SetName(name []byte) {
	sysschobjs.Name = name
}

func (sysschobjs Sysschobjs) GetName() string {
	return utils.DecodeUTF16(sysschobjs.Name)
}

func (sysschobjs Sysschobjs) GetData() (any, any) {
	return sysschobjs.Id, Result[string, string, uint64, uint, uint, uint, uint]{utils.DecodeUTF16(sysschobjs.Name),
		sysschobjs.GetTableType(), 0, 0, 0, 0, 0}

}

func (sysschobjs Sysschobjs) ShowData() {
	fmt.Printf("sysschobjs %d %s\n", sysschobjs.Id, sysschobjs.GetName())
}

func (sysschobjs Sysschobjs) GetTableType() string {
	return TableType[strings.TrimSpace(string(sysschobjs.Type[:]))]
}
