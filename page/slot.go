package page

import (
	"MSSQLParser/utils"
	"encoding/binary"
)

type Slot struct {
	Order                int
	Offset               uint16
	Deleted              bool
	ActualDataRowSize    uint16
	AllocatedDataRowSize uint16
}

type Slots []Slot
type SortedSlotsByOffset []Slot

func (s SortedSlotsByOffset) Len() int {
	return len(s)

}

func (s SortedSlotsByOffset) Less(i, j int) bool {
	return int(s[i].Offset) < int(s[j].Offset)
}

func (s SortedSlotsByOffset) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type SortedSlotsByOrder []Slot

func (s SortedSlotsByOrder) Len() int {
	return len(s)

}

func (s SortedSlotsByOrder) Less(i, j int) bool {
	return int(s[i].Order) < int(s[j].Order)
}

func (s SortedSlotsByOrder) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func retrieveSlots(data []byte) Slots {
	slots := make(Slots, len(data)/2)

	pos := 0

	for idx := 0; idx < binary.Size(data); idx += 2 {
		slots[pos] = Slot{Order: len(slots) - pos - 1,
			Offset:  utils.ToUint16(data[idx : idx+2]),
			Deleted: false,
		}
		pos++

	}

	return slots
}
