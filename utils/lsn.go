package utils

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

type LSN struct {
	P1 uint32
	P2 uint32
	P3 uint16 // record id within log block
}

func (lsn LSN) IsZeroed() bool {
	return lsn.P1 == 0 && lsn.P2 == 0 && lsn.P3 == 0
}

func (lsn LSN) ToBytes() []byte {
	buf := make([]byte, 10)
	binary.LittleEndian.PutUint32(buf[0:4], lsn.P1)
	binary.LittleEndian.PutUint32(buf[4:8], lsn.P2)
	binary.LittleEndian.PutUint16(buf[8:10], lsn.P3)
	return buf
}

func (a LSN) IsGreaterEqual(b LSN) bool {
	if a.P1 != b.P1 {
		return a.P1 > b.P1
	}
	if a.P2 != b.P2 {
		return a.P2 > b.P2
	}
	return a.P3 >= b.P3
}

func (lsn LSN) IsLess(smallerLSN LSN) bool {
	if lsn.P1 < smallerLSN.P1 {
		return true
	} else if lsn.P1 == smallerLSN.P1 && lsn.P2 < smallerLSN.P2 {
		return true
	} else if lsn.P1 == smallerLSN.P1 && lsn.P2 == smallerLSN.P2 && lsn.P3 < smallerLSN.P3 {
		return true
	} else {
		return false
	}
}

func (lsn LSN) Equals(otherLSN LSN) bool {
	return lsn.P1 == otherLSN.P1 && lsn.P2 == otherLSN.P2 && lsn.P3 == otherLSN.P3
}

func (lsn *LSN) Increment() {
	lsn.P3 += 1
}

func (lsn LSN) ToStr() string {

	p1 := fillPrefixWithZeros(strconv.FormatUint(uint64(lsn.P1), 16), 8)
	p2 := fillPrefixWithZeros(strconv.FormatUint(uint64(lsn.P2), 16), 8)
	p3 := fillPrefixWithZeros(strconv.FormatUint(uint64(lsn.P3), 16), 4)
	return fmt.Sprintf("%v:%v:%v", p1, p2, p3)
}
