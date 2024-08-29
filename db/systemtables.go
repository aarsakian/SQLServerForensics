package db

import (
	mslogger "MSSQLParser/logger"
	"MSSQLParser/page"
	"MSSQLParser/utils"
	"fmt"
	"strings"
)

var SystemTablesFlags = map[string]int32{
	"syscolpars": 0x00000029, "sysrowsets": 0x00000005, "sysiscols": 0x00000037,
	"sysallocationunits": 0x00000007, "sysidxstats": 0x000036,
	"sysschobjs": 0x00000022, "sysrscols": 0x00000003, "sysfiles": 0x00000008}

var TableType = map[string]string{"AF": "Aggregate function (CLR)", "U": "User Table", "S": "System Table",
	"V": "View", "P": "Stored Procedure", "TT": "Table Type", "UQ": "Unique Constraint", "C": "Check constraint",
	"F": "Foreign Key", "FS": "Assembly (CLR) Scalar function", "FN": "Scalar Function", "FT": "Assembly (CLR) Table-Valued function"}

type TablesInfo map[int32]Sysschobjs

type ColumnsInfo map[int32][]SysColpars

type TablesPartitions map[int32][]SysRowSets

type IndexesInfo map[int32][]SysIdxStats

type TablesAllocations map[uint64][]SysAllocUnits //OwnerId

type ColumnsPartitions map[uint64]SysRsCols //rowsetid ->

type ColumnsStatistics map[int32]SysIsCols

type SysIsCols []SysIsCol

type SysRsCols []SysRsCol

type SysFiles []SysFile

type SysObjValues struct {
	Valclass uint8
	Objid    int32
	Subobjid int32
	Valnum   int32
	Value    SqlVariant
	Imageval []byte
}

type SysFile struct {
	Status   int32
	FileId   uint16
	Name     [256]byte
	Filename [256]byte
}

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
	OwnerId    uint64  //9-17 rowsetid
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
	Id          int32  //4 -8  //objectID
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
	Dflt        uint32 //default constraint
	Chk         uint32
	Idtval      []byte
	Name        []byte
}

// a row for every index or statistics
type SysIdxStats struct {
	Id        int32  //objectID
	Indid     uint32 // index id ID of the index: 0 = Heap; 1 = Clustered index; >1 = Nonclustered index
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

type SysIsCol struct {
	Idmajor   int32
	Idminor   uint32
	Subid     uint32
	Status    uint32
	Intprop   uint32
	Tinyprop1 uint8
	Tinyprop2 uint8
	Tinyprop3 uint8
	Tinyprop4 uint8
}

// tracks col modifications using Rcmodified as a counter
type SysRsCol struct {
	Rsid        uint64 //1-8 partition id
	Rscolid     uint32 //8-12 column id
	Hbcolid     uint32 //12 - 16 column order in the index
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

// a row for each partition rowset for an index or a heap
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

func (sysschobjs Sysschobjs) GetTableType() string {
	return TableType[strings.TrimSpace(string(sysschobjs.Type[:]))]
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

func (syscolpars SysColpars) GetName() string {
	return utils.DecodeUTF16(syscolpars.Name)

}
func (sysidxstats SysIdxStats) GetName() string {
	return utils.DecodeUTF16(sysidxstats.Name)

}

func (sysfile SysFile) GetName() string {
	return utils.DecodeUTF16(sysfile.Name[:])
}

func (sysfile SysFile) GetFileName() string {
	return utils.DecodeUTF16(sysfile.Filename[:])
}

func (sysallocationunits SysAllocUnits) GetRootPageId() uint32 {
	return utils.ToUint32(sysallocationunits.Pgroot[:4])
}

func (sysallocationunits SysAllocUnits) GetFirstPageId() uint32 {
	return utils.ToUint32(sysallocationunits.PgFirst[:4])
}

func (sysschobjs Sysschobjs) GetName() string {
	return utils.DecodeUTF16(sysschobjs.Name)
}

func (sysrscol SysRsCol) GetLeafOffset() int16 {
	return int16(sysrscol.Offset & 0xffff)

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
		return "Type not Found"
	}

}

func (tablesInfo TablesInfo) Populate(datarows page.DataRows) {
	for _, datarow := range datarows {
		sysschobjs := new(Sysschobjs)
		utils.Unmarshal(datarow.FixedLenCols, sysschobjs)
		if datarow.VarLenCols != nil {
			for idx, datacol := range *datarow.VarLenCols {
				if idx == 0 {
					sysschobjs.Name = datacol.Content
				}
			}
		}

		tablesInfo[sysschobjs.Id] = *sysschobjs
	}

}

func (columnsStats ColumnsStatistics) Populate(datarows page.DataRows) {
	for _, datarow := range datarows {
		sysiscol := new(SysIsCol)
		utils.Unmarshal(datarow.FixedLenCols, sysiscol)
		columnsStats[sysiscol.Idmajor] = append(columnsStats[sysiscol.Idmajor],
			*sysiscol)
	}
}

func (sysfiles SysFiles) Populate(datarows page.DataRows) {
	for idx, datarow := range datarows {
		sysfile := new(SysFile)
		utils.Unmarshal(datarow.FixedLenCols, sysfile)
		sysfiles[idx] = *sysfile
	}

}

func (sysallocunits SysAllocUnits) GetId() uint64 {
	return utils.ToUint64(sysallocunits.Auid[:])
}

func (indexesInfo IndexesInfo) Populate(datarows page.DataRows) {
	for _, datarow := range datarows {
		sysidxstats := new(SysIdxStats)
		utils.Unmarshal(datarow.FixedLenCols, sysidxstats)
		if datarow.VarLenCols != nil {
			for idx, datacol := range *datarow.VarLenCols {
				if idx == 0 {
					sysidxstats.Name = datacol.Content
				}
			}
		}
		indexesInfo[sysidxstats.Id] = append(indexesInfo[sysidxstats.Id], *sysidxstats)
	}

}

func (columnsinfo ColumnsInfo) Populate(datarows page.DataRows) {
	for _, datarow := range datarows {
		syscolpars := new(SysColpars)
		utils.Unmarshal(datarow.FixedLenCols, syscolpars)
		if datarow.VarLenCols != nil {
			for idx, datacol := range *datarow.VarLenCols {
				if idx == 0 {
					syscolpars.Name = datacol.Content
				} else {
					syscolpars.Idtval = datacol.Content
				}
			}
		}

		columnsinfo[syscolpars.Id] = append(columnsinfo[syscolpars.Id], *syscolpars)
	}

}

func (tablespartitions TablesPartitions) Populate(datarows page.DataRows) {

	for _, datarow := range datarows {
		sysrowsets := new(SysRowSets)
		utils.Unmarshal(datarow.FixedLenCols, sysrowsets)
		tablespartitions[sysrowsets.Idmajor] = append(tablespartitions[sysrowsets.Idmajor],
			*sysrowsets)
	}
}

func (tablesallocations TablesAllocations) Populate(datarows page.DataRows) {

	for _, datarow := range datarows {
		sysallocunits := new(SysAllocUnits)
		utils.Unmarshal(datarow.FixedLenCols, sysallocunits)
		tablesallocations[sysallocunits.OwnerId] = append(tablesallocations[sysallocunits.OwnerId],
			*sysallocunits)
	}
}

func (columnsPartitions ColumnsPartitions) Populate(datarows page.DataRows) {

	for _, datarow := range datarows {
		sysrscol := new(SysRsCol)
		utils.Unmarshal(datarow.FixedLenCols, sysrscol)
		if datarow.VarLenCols != nil {
			for idx, datacol := range *datarow.VarLenCols {
				if idx == 0 {
					sysrscol.Olguid = datacol.Content
				}

			}
		}

		columnsPartitions[sysrscol.Rsid] =
			append(columnsPartitions[sysrscol.Rsid], *sysrscol)
	}
}

func (sysiscols SysIsCols) filterByIndexId(indexid uint32) SysIsCols {
	return utils.Filter(sysiscols, func(sysiscol SysIsCol) bool {
		return sysiscol.Idminor == indexid

	})
}

func (sysrscols SysRsCols) filterByIndexId(indexid uint32) SysRsCol {
	return utils.Filter(sysrscols, func(sysrscol SysRsCol) bool {
		return sysrscol.Hbcolid == indexid
	})[0]
}
