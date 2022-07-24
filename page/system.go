package page

import (
	"MSSQLParser/utils"
	"fmt"
)

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
	Id          uint32 //4 -8
	Number      uint16 //8-10
	ColOrder    uint16 //10 -12
	Unknown     [2]byte
	Xtype       uint8  // 14 sys.sysscalartypes.xtype.
	Utype       uint32 //15-19 sys.sysscalartypes.id
	Colsize     uint16 //19-21
	Prec        uint8
	Scale       uint8
	CollationID uint32
	Status      uint32
	Maxinrow    uint16
	Xmlns       uint32
	Dflt        uint32
	Chk         uint32
	Idtval      []byte
	Name        []byte
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

type SysRowSets struct {
	Rowsetid   uint64
	Ownertype  uint8
	Idmajor    uint32
	Idminor    uint32
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

//stores table/ contstraints information
type Sysschobjs struct {
	Id       int32   //0-4
	Nsid     uint32  //4-8
	Nsclass  uint8   //8-9
	Status   uint32  //9-13
	Type     [2]byte //13-15 bytes
	Pid      uint32  //15-19
	Intprop  uint32  //19-21
	Created  [8]byte //21-29
	Modified [8]byte //29-37
	Status2  uint32  //37-41
	Name     []byte
}

type Result[F, S, T, FH any] struct {
	First  F
	Second S
	Third  T
	Fourth FH
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
	return int32(sysrowsets.Idmajor), sysrowsets.Rowsetid
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
	return sysallocunits.OwnerId, uint32(sysallocunits.getPageId())
}

func (sysallocunits SysAllocUnits) getIndexId() int {
	return utils.ToInt(sysallocunits.Auid[:]) >> 48
}

func (sysallocunits SysAllocUnits) getPageId() int {
	indexId := sysallocunits.getIndexId()
	return (utils.ToInt(sysallocunits.Auid[:]) - (indexId << 48)) >> 16
}
func (sysallocunits SysAllocUnits) ShowData() {

	pageId := sysallocunits.getPageId()
	fmt.Printf("sysalloc Page ObjectId %d AllocUID %d %d  PartitionId  %d  %x %x %d\n", pageId, utils.ToInt(sysallocunits.Auid[:]),
		sysallocunits.Type, sysallocunits.OwnerId,
		sysallocunits.PgFirst, sysallocunits.PgFirstIAM, sysallocunits.PcData)
}

func (syscolpars SysColpars) GetName() string {

	return utils.DecodeUTF16(syscolpars.Name)

}

func (syscolpars SysColpars) GetData() (any, any) {
	return int32(syscolpars.Id),
		Result[string, string, uint16, uint16]{syscolpars.GetName(),
			syscolpars.GetType(), syscolpars.Colsize, syscolpars.ColOrder}

}

func (syscolpars *SysColpars) SetName(name []byte) {
	syscolpars.Name = name
}

func (syscolpars SysColpars) GetType() string {
	if syscolpars.Xtype == 0x7f {
		return "bigint"
	} else if syscolpars.Xtype == 0xAD {
		return "binary"
	} else if syscolpars.Xtype == 0x6A {
		return "bit"
	} else if syscolpars.Xtype == 0xAF {
		return "char"
	} else if syscolpars.Xtype == 0x3D {
		return "datetime"
	} else if syscolpars.Xtype == 0x28 {
		return "date"
	} else if syscolpars.Xtype == 0x38 {
		return "int"
	} else if syscolpars.Xtype == 0xEF {
		return "nchar"
	} else if syscolpars.Xtype == 0x06 {
		return "numeric"
	} else if syscolpars.Xtype == 0x23 {
		return "text"
	} else if syscolpars.Xtype == 0x24 {
		return "timestamp"
	} else if syscolpars.Xtype == 0x30 {
		return "tinyint"
	} else if syscolpars.Xtype == 0xA7 {
		return "varchar"
	} else {
		return "Type not Found"
	}

}

func (syscolpars SysColpars) ShowData() {
	fmt.Printf("syscolpars id %d  len %d  number %d Colorder %d %s \n",
		syscolpars.Id, syscolpars.Colsize, syscolpars.Number, syscolpars.ColOrder,
		syscolpars.GetName())
}

func (sysschobjs *Sysschobjs) SetName(name []byte) {
	sysschobjs.Name = name
}

func (sysschobjs Sysschobjs) GetName() string {
	return utils.DecodeUTF16(sysschobjs.Name)
}

func (sysschobjs Sysschobjs) GetData() (any, any) {
	return sysschobjs.Id, utils.DecodeUTF16(sysschobjs.Name)

}

func (sysschobjs Sysschobjs) ShowData() {
	fmt.Printf("sysschobjs %d %s\n", sysschobjs.Id, sysschobjs.GetName())
}
