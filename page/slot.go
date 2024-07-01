package page

import (
	"MSSQLParser/utils"
	"encoding/binary"
	"sort"
)

type Slot struct {
	Order  int
	Offset uint16
}

type Slots []Slot
type SortedSlots []Slot

func (s SortedSlots) Len() int {
	return len(s)

}

func (s SortedSlots) Less(i, j int) bool {
	return int(s[i].Offset) < int(s[j].Offset)
}

func (s SortedSlots) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func retrieveSortedSlots(data []byte) Slots {
	var slots Slots

	for idx := 0; idx < binary.Size(data); idx += 2 {
		slot := Slot{Offset: utils.ToUint16(data[idx : idx+2])}
		slots = append(slots, slot)

	}

	for idx := range slots {
		slots[idx].Order = len(slots) - idx - 1
	}
	sort.Sort(SortedSlots(slots))
	return slots
}
