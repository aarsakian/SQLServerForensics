package page

import (
	datac "MSSQLParser/data"
	"reflect"
)

type ChangeStatus string

const (
	Added     ChangeStatus = "added"
	Removed   ChangeStatus = "removed"
	Changed   ChangeStatus = "changed"
	Unchanged ChangeStatus = "unchanged"
)

type FieldDiff struct {
	Field  string
	Before any
	After  any
}

type SlotDiff struct {
	Index  int
	Before *Slot
	After  *Slot
}

type RowDiff struct {
	Index  int
	Field  string
	Before any
	After  any
}

type MapDiff struct {
	Map    string
	Index  int
	PageID uint32
	Field  string
	Before any
	After  any
}

type PageComparison struct {
	PageID      uint32
	AllocUnitID uint64
	Status      ChangeStatus
	HeaderDiffs []FieldDiff
	SlotDiffs   []SlotDiff
	RowDiffs    []RowDiff
	MapDiffs    []MapDiff
}

func ComparePages(base, target Page) PageComparison {
	comparison := PageComparison{
		PageID:      target.Header.PageId,
		AllocUnitID: target.Header.GetMetadataAllocUnitId(),
		Status:      Unchanged,
	}

	compareHeader(&comparison, base.Header, target.Header)
	compareSlots(&comparison, base.Slots, target.Slots)
	compareRows(&comparison, base.DataRows, target.DataRows)
	compareMaps(&comparison, base, target)

	if len(comparison.HeaderDiffs) != 0 || len(comparison.SlotDiffs) != 0 ||
		len(comparison.RowDiffs) != 0 || len(comparison.MapDiffs) != 0 {
		comparison.Status = Changed
	}
	return comparison
}

func compareHeader(comparison *PageComparison, base, target Header) {
	addField := func(name string, before, after any) {
		if !reflect.DeepEqual(before, after) {
			comparison.HeaderDiffs = append(comparison.HeaderDiffs,
				FieldDiff{Field: name, Before: before, After: after})
		}
	}

	addField("Version", base.Version, target.Version)
	addField("Type", base.Type, target.Type)
	addField("TypeFlagBits", base.TypeFlagBits, target.TypeFlagBits)
	addField("Level", base.Level, target.Level)
	addField("FlagBits", base.FlagBits, target.FlagBits)
	addField("IndexId", base.IndexId, target.IndexId)
	addField("PrevPage", base.PrevPage, target.PrevPage)
	addField("PreviousFileId", base.PreviousFileId, target.PreviousFileId)
	addField("PMinLen", base.PMinLen, target.PMinLen)
	addField("NextPage", base.NextPage, target.NextPage)
	addField("NextPageFileId", base.NextPageFileId, target.NextPageFileId)
	addField("SlotCnt", base.SlotCnt, target.SlotCnt)
	addField("ObjectId", base.ObjectId, target.ObjectId)
	addField("FreeCnt", base.FreeCnt, target.FreeCnt)
	addField("FreeData", base.FreeData, target.FreeData)
	addField("PageId", base.PageId, target.PageId)
	addField("FragId", base.FragId, target.FragId)
	addField("LSN", base.LSN, target.LSN)
	addField("XactReserved", base.XactReserved, target.XactReserved)
	addField("GhostRecCnt", base.GhostRecCnt, target.GhostRecCnt)
	addField("TornBits", base.TornBits, target.TornBits)
	addField("Checksum", base.Checksum, target.Checksum)
}

func compareSlots(comparison *PageComparison, base, target Slots) {
	for index := 0; index < len(base) || index < len(target); index++ {
		var before, after *Slot
		if index < len(base) {
			before = &base[index]
		}
		if index < len(target) {
			after = &target[index]
		}
		if !reflect.DeepEqual(before, after) {
			comparison.SlotDiffs = append(comparison.SlotDiffs,
				SlotDiff{Index: index, Before: before, After: after})
		}
	}
}

func compareRows(comparison *PageComparison, base, target datac.DataRows) {
	for index := 0; index < len(base) || index < len(target); index++ {
		var before, after *datac.DataRow
		if index < len(base) {
			before = &base[index]
		}
		if index < len(target) {
			after = &target[index]
		}

		addField := func(name string, beforeValue, afterValue any) {
			if !reflect.DeepEqual(beforeValue, afterValue) {
				comparison.RowDiffs = append(comparison.RowDiffs,
					RowDiff{Index: index, Field: name, Before: beforeValue, After: afterValue})
			}
		}

		if before != nil && after != nil {
			addField("StatusA", before.StatusA, after.StatusA)
			addField("StatusB", before.StatusB, after.StatusB)
			addField("NofColsOffset", before.NofColsOffset, after.NofColsOffset)
			addField("FixedLenCols", before.FixedLenCols, after.FixedLenCols)
			addField("NumberOfCols", before.NumberOfCols, after.NumberOfCols)
			addField("NullBitmap", before.NullBitmap, after.NullBitmap)
			addField("NumberOfVarLengthCols", before.NumberOfVarLengthCols, after.NumberOfVarLengthCols)
			addField("VarLengthColEndOffsets", before.VarLengthColEndOffsets, after.VarLengthColEndOffsets)
			addField("VersioningInfo", before.VersioningInfo, after.VersioningInfo)
			addField("VarLenCols", before.VarLenCols, after.VarLenCols)
			addField("Carved", before.Carved, after.Carved)
			addField("SlotIdx", before.SlotIdx, after.SlotIdx)
		}
		if before == nil && after != nil {
			addField("DataRow", nil, after)
		}
		if before != nil && after == nil {
			addField("DataRow", before, nil)
		}
	}
}

func compareMaps(comparison *PageComparison, base, target Page) {
	comparePFS(comparison, base.PFSPage, target.PFSPage)
	compareGAM(comparison, base.GAMExtents, target.GAMExtents)
	compareSGAM(comparison, base.SGAMExtents, target.SGAMExtents)
	compareIAM(comparison, base.IAM, target.IAM)
	compareDiffMap(comparison, base.DiffMapExtents, target.DiffMapExtents)
	compareBulkChangeMap(comparison, base.BulkChangeMapExtents, target.BulkChangeMapExtents)
}

func addMapField(comparison *PageComparison, name string, index int, field string, before, after any) {
	addMapFieldAtPage(comparison, name, index, 0, field, before, after)
}

func addMapFieldAtPage(comparison *PageComparison, name string, index int, pageID uint32, field string, before, after any) {
	if !reflect.DeepEqual(before, after) {
		comparison.MapDiffs = append(comparison.MapDiffs,
			MapDiff{Map: name, Index: index, PageID: pageID, Field: field, Before: before, After: after})
	}
}

func addMapEntryDiff(comparison *PageComparison, name string, index int, before, after any, entryIndex int) {
	var beforeEntry, afterEntry any
	if beforeValue := reflect.ValueOf(before); beforeValue.IsValid() && beforeValue.Kind() == reflect.Pointer && !beforeValue.IsNil() && entryIndex < beforeValue.Elem().Len() {
		beforeEntry = beforeValue.Elem().Index(entryIndex).Interface()
	}
	if afterValue := reflect.ValueOf(after); afterValue.IsValid() && afterValue.Kind() == reflect.Pointer && !afterValue.IsNil() && entryIndex < afterValue.Elem().Len() {
		afterEntry = afterValue.Elem().Index(entryIndex).Interface()
	}
	if !reflect.DeepEqual(beforeEntry, afterEntry) {
		comparison.MapDiffs = append(comparison.MapDiffs, MapDiff{Map: name, Index: index, Field: "Entry", Before: beforeEntry, After: afterEntry})
	}
}

