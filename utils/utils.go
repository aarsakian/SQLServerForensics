package utils

import (
	mslogger "MSSQLParser/logger"
	"os"
	"path/filepath"
	"time"

	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

var LeapYear = map[int]int{1: 1, 2: 31, 3: 60, 4: 91, 5: 121, 6: 152, 7: 182, 8: 213, 9: 244, 10: 274, 11: 305, 12: 335}

var Year = map[int]int{1: 1, 2: 31, 3: 59, 4: 90, 5: 120, 6: 151, 7: 181, 8: 212, 9: 243, 10: 273, 11: 304, 12: 334}

type Record []string
type Records [][]string

type RowId struct {
	PageId     uint32
	FileId     uint16
	SlotNumber uint16
}

type LSN struct {
	P1 uint32
	P2 uint32
	P3 uint16 // record id within log block
}

type TransactionID struct {
	P1 uint16
	P2 uint32
}

type Auid struct {
	UniqueId uint16
	ObjectId uint32
	Zeros    uint32
}

type Images [][]byte

func (lsn LSN) IsGreater(smallerLSN LSN) bool {
	if lsn.P1 > smallerLSN.P1 {
		return true
	} else if lsn.P1 == smallerLSN.P1 && lsn.P2 > smallerLSN.P2 {
		return true
	} else if lsn.P1 == smallerLSN.P1 && lsn.P2 == smallerLSN.P2 && lsn.P3 > smallerLSN.P3 {
		return true
	} else {
		return false
	}
}

func (lsn *LSN) Increment() {
	lsn.P3 += 1
}

func (rowid RowId) ToStr() string {
	fileID := fillPrefixWithZeros(strconv.FormatUint(uint64(rowid.FileId), 10), 4)
	pageID := fillPrefixWithZeros(strconv.FormatUint(uint64(rowid.PageId), 10), 8)
	slotID := fillPrefixWithZeros(strconv.FormatUint(uint64(rowid.SlotNumber), 10), 4)
	return fmt.Sprintf("%v:%v:%v", fileID, pageID, slotID)
}

func (lsn LSN) ToStr() string {

	p1 := fillPrefixWithZeros(strconv.FormatUint(uint64(lsn.P1), 16), 8)
	p2 := fillPrefixWithZeros(strconv.FormatUint(uint64(lsn.P2), 16), 8)
	p3 := fillPrefixWithZeros(strconv.FormatUint(uint64(lsn.P3), 16), 4)
	return fmt.Sprintf("%v:%v:%v", p1, p2, p3)
}

func (transactionID TransactionID) ToStr() string {
	p1 := fillPrefixWithZeros(strconv.FormatUint(uint64(transactionID.P1), 16), 4)
	p2 := fillPrefixWithZeros(strconv.FormatUint(uint64(transactionID.P2), 16), 8)
	return fmt.Sprintf("%v:%v", p1, p2)
}

// Datetime2: 8 bytes rtl reading first 5 time unit intervals since midnight,last 3 (left) how many days have passed since 0001/01/01
// 0x07 prefix time unit 100ns, 0x06 1 micro second intervals
func DateTime2Tostr(data []byte) string {
	return DateToStr(data[5:8])

}

// 1 byte for precision 1 byte for scale
func NumericToStr(data []byte) string {
	return ""
}

func LocateLDFfile(mdffile string) (string, error) {
	dir, fname := filepath.Split(mdffile)
	fname = strings.Split(fname, ".")[0]

	entries, e := os.ReadDir(dir)
	if e != nil {
		fmt.Printf("Error listing ldf folder%s", e)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".ldf" {
			continue
		}
		if strings.Contains(entry.Name(), fname) {
			return filepath.Join(dir, entry.Name()), nil
		}

	}
	return "", errors.New("LDF file not found")
}

func isLeapYear(year uint) bool {
	if year%4 == 0 && year%100 != 0 || year%400 == 0 {
		return true
	}
	return false

}

func CheckLenBefore(data []byte, functocall func([]byte) string) string {
	if len(data) < 8 {
		dst := make([]byte, 8)
		copy(dst, data)
		data = dst

	}
	return functocall(data)
}

func RealToStr(data []byte, precision uint8, scale uint8) string {
	//reverse bytes
	//Decimal 10.3 allocate bytes to accomodate for precision e.g. 1.7 = 1700 = a406  (Little Endian) 4 bytes mini length
	sign := ""
	if uint(data[0]) == 0 { // 1 = positive
		sign = "-"
	}
	val := strconv.FormatUint(uint64(ToInt32(data[1:])), 10)

	dotPos := len(val) - int(scale)
	if dotPos < 0 {
		mslogger.Mslogger.Warning(fmt.Sprintf("scale %d less than value length %d", scale, len(val)))
		return fmt.Sprintf("%s%s", sign, val)
	} else {
		return fmt.Sprintf("%s%s.%s", sign, val[:dotPos], val[dotPos:])
	}

}

// 1: the signed bit
// 2 to 12: the exponent, which is offset against a bias 2^1023
// 13 to 64: the significand (also known as the mantissa)
// 52 bits for the fraction use negative power to 2
// finaly multiply with exponent
func FloatToStr(data []byte) string {
	var bitrepresentation strings.Builder
	for _, byteval := range Bytereverse(data[6:8]) {

		bitrepresentation.WriteString(fillPrefixWithZeros(
			strconv.FormatUint(uint64(byteval), 2), 8))
	}

	intval, _ := strconv.ParseUint(bitrepresentation.String()[1:12], 2, 16)
	exponent := math.Pow(2, float64(intval-1023))

	mantissaSum := 0.0
	var mantissa strings.Builder
	mantissa.WriteString(fillPrefixWithZeros(strconv.FormatUint(uint64(data[7]), 2), 8)[4:8])
	for _, byteval := range Bytereverse(data[0:6]) {

		mantissa.WriteString(fillPrefixWithZeros(strconv.FormatUint(uint64(byteval), 2), 8))
	}

	for pos, bitval := range mantissa.String() {
		mantissaSum += float64(bitval-48) * math.Pow(2, float64(-1*(pos+1)))

	}
	mantissaSum += 1
	if data[7]&0x80 == 1 { //sing check
		return strconv.FormatFloat(-1*exponent*mantissaSum, 'E', -1, 64)
	} else {
		return strconv.FormatFloat(exponent*mantissaSum, 'E', -1, 64)
	}

}

func fillPrefixWithZeros(bitval string, targetLen int) string {
	// add missing zeros

	for len(bitval) < targetLen {
		bitval = "0" + bitval
	}
	return bitval
}

func Bytereverse(barray []byte) []byte { //work with indexes
	//  fmt.Println("before",barray)
	for i, j := 0, len(barray)-1; i < j; i, j = i+1, j-1 {

		barray[i], barray[j] = barray[j], barray[i]

	}
	return barray

}

func DateToStr(data []byte) string {
	/*DATE is the byte-reversed number of days since the year 0001-01-01, stored as three bytes. */
	var day int
	var month int
	var b bytes.Buffer
	b.Grow(4)

	b.Write(data)
	b.Write([]byte{0x00})
	daysSince0001 := ToInt32(b.Bytes())
	years := int(math.Floor(float64(daysSince0001) / float64(365.24)))
	nofLeapYears := years/4 - years/100 + years/400
	daysInTheYear := int(daysSince0001-(years-nofLeapYears)*365-nofLeapYears*366) + 1
	years = years + 1
	if isLeapYear(uint(years)) {
		month = int(float64(daysInTheYear)/30.41667 + 1)
		day = daysInTheYear - LeapYear[month]
	} else {
		month = int(math.Round(float64(daysInTheYear)/30.5 + 1))
		day = daysInTheYear - Year[month]
	}
	return fmt.Sprintf("%d-%d-%d", day, month, years)

}

/*
Datetime is stored as two 4-byte values: the first (for
the date) being the number of days before or after the base date of January 1, 1900, and the second
(for the time) being the number of clock ticks after midnight, with each tick representing 3.33 milliseconds, or 1/300 of a second.
*/
func DateTimeTostr(data []byte) string {
	day, month, years, hours, minutes, seconds, msecs := parseDateTime(data)
	return fmt.Sprintf("%d/%d/%d %d:%02d:%02d.%03d", day, month, years, hours, minutes, seconds, msecs)
}

func DateTimeToObj(data []byte) time.Time {
	day, month, years, hours, minutes, seconds, msecs := parseDateTime(data)
	return time.Date(years, time.Month(month), day, hours, minutes, seconds, msecs, time.UTC)
}

func ParseSmallDateTime(data []byte) string {
	var day int
	var month int

	if len(data) < 8 {
		dst := make([]byte, 8)
		copy(dst, data)
		data = dst

	}
	daysSince1900 := ToInt32(data[2:4])
	years := int(math.Floor(float64(daysSince1900) / float64(365.24)))

	nofLeapYears := (years+1900)/4 - (years+1900)/100 + (years+1900)/400 - (1900/4 - 1900/100 + 1900/400)
	daysInTheYear := int(daysSince1900-(years-nofLeapYears)*365-nofLeapYears*366) + 1

	if isLeapYear(uint(years + 1900)) {
		month = int(float64(daysInTheYear)/30.41667 + 1)
		day = daysInTheYear - LeapYear[month] + 1
	} else {
		month = int(float64(daysInTheYear)/30.5 + 1)
		day = daysInTheYear - Year[month]
	}

	timePart := ToUint32(data[0:2])
	hours := int(float64((timePart / (300 * 60 * 60)) % 24))
	minutes := int(float64((timePart / (300 * 60)) % 60))
	seconds := 0
	return fmt.Sprintf("%d/%d/%d %d:%02d:%02d", day, month, years+1900, hours, minutes, seconds)

}

func parseDateTime(data []byte) (int, int, int, int, int, int, int) {

	var day int
	var month int

	if len(data) < 8 {
		dst := make([]byte, 8)
		copy(dst, data)
		data = dst

	}
	daysSince1900 := ToInt32(data[4:8])
	years := int(math.Floor(float64(daysSince1900) / float64(365.24)))

	nofLeapYears := (years+1900)/4 - (years+1900)/100 + (years+1900)/400 - (1900/4 - 1900/100 + 1900/400)
	daysInTheYear := int(daysSince1900-(years-nofLeapYears)*365-nofLeapYears*366) + 1

	if isLeapYear(uint(years + 1900)) {
		month = int(float64(daysInTheYear)/30.41667 + 1)
		day = daysInTheYear - LeapYear[month] + 1
	} else {
		month = int(float64(daysInTheYear)/30.5 + 1)
		day = daysInTheYear - Year[month]
	}

	timePart := ToUint32(data[0:4])
	hours := int(float64((timePart / (300 * 60 * 60)) % 24))
	minutes := int(float64((timePart / (300 * 60)) % 60))
	seconds := int(float64((timePart / 300) % 60))
	_, msecs := math.Modf(float64(timePart) / 300)
	return day, month, years + 1900, hours, minutes, seconds, int(1000 * msecs)
}

func MoneyToStr(data []byte) string {
	//MONEY both store a value up to 15 digits
	val := strconv.FormatInt(ToInt64(data), 10)

	if len(val) > 4 { //always if data >0
		return fmt.Sprintf("%s.%s", val[:len(val)-4], val[len(val)-4:])
	} else {
		return "0.0000"
	}

}

//SMALLMONEY can only store a maximum of six digits before the decimal point.

func DecimalToStr(data []byte, precision uint8, scale uint8) string {
	//reverse bytes
	//Decimal 10.3 allocate bytes to accomodate for precision e.g. 1.7 = 1700 = a406  (Little Endian) 4 bytes mini length
	sign := ""
	if uint(data[0]) == 0 { // 1 = positive
		sign = "-"
	}
	val := strconv.FormatUint(uint64(ToInt32(data[1:])), 10)

	dotPos := len(val) - int(scale)
	if dotPos < 0 {
		mslogger.Mslogger.Warning(fmt.Sprintf("scale %d less than value length %d", scale, len(val)))
		return fmt.Sprintf("%s%s", sign, val)
	} else {
		return fmt.Sprintf("%s%s.%s", sign, val[:dotPos], val[dotPos:])
	}

}

func ToStructAuid(data []byte) Auid {

	var auid Auid
	Unmarshal(data, &auid)
	return auid

}

func RemoveSignBit(val int16) int16 {
	return int16(uint16(val<<1) >> 1)
}

func ToInt64(data []byte) int64 {
	var temp int64
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &temp)
	return temp
}

func ToInt32(data []byte) int {
	var temp int32
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &temp)
	return int(temp)
}

func ToInt16(data []byte) int {
	var temp int16
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &temp)
	return int(temp)
}

func ToInt8(data []byte) int {
	var temp int8
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &temp)
	return int(temp)
}

func ToUint16(data []byte) uint16 {
	var temp uint16
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &temp)
	return uint16(temp)
}

func ToUint32(data []byte) uint32 {
	var temp uint32
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &temp)
	return uint32(temp)
}

func ToBInt32(data []byte) int32 {
	var temp int32
	binary.Read(bytes.NewBuffer(data), binary.BigEndian, &temp)
	return int32(temp)
}

func ToBUint32(data []byte) uint32 {
	var temp uint32
	binary.Read(bytes.NewBuffer(data), binary.BigEndian, &temp)
	return uint32(temp)
}

func ToUint64(data []byte) uint64 {
	var temp uint64
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &temp)
	return uint64(temp)
}

func addMissingBits(bitval string, targetLen int, pos int) string {
	// add missing zeros
	var maxnofZeros int
	if targetLen < 8*(pos+1) {
		maxnofZeros = targetLen % 8
	} else {
		maxnofZeros = 8
	}
	for len(bitval) < maxnofZeros {
		bitval = "0" + bitval
	}
	return bitval
}

func HasFlagSet(bitmap []byte, flagPos int, nofCols int) bool {
	//index starts from left to right 0-7, 8-16
	//bitmap right to left
	var bitflag byte
	var bitrepresentation string
	for valpos, val := range bitmap {
		bitval := addMissingBits(strconv.FormatUint(uint64(val), 2), nofCols, valpos)
		bitrepresentation = strings.Join([]string{bitval, bitrepresentation}, "")
	}

	if len(bitrepresentation) > nofCols { // remove not needed bits
		startOffset := len(bitrepresentation) - nofCols
		endOffset := nofCols - flagPos
		bitflag = bitrepresentation[startOffset+endOffset-1 : startOffset+endOffset][0]
	} else {
		bitflag = bitrepresentation[nofCols-flagPos]
	}

	return bitflag == 49 // ascii 49 = 1

}

func DecodeUTF16(b []byte) string {
	utf := make([]uint16, (len(b)+(2-1))/2) // utf-16 2 bytes for each char
	for i := 0; i+(2-1) < len(b); i += 2 {
		utf[i/2] = binary.LittleEndian.Uint16(b[i:])
	}
	if len(b)/2 < len(utf) { // the "error" Rune or "Unicode replacement character"
		utf[len(utf)-1] = utf8.RuneError
	}
	return string(utf16.Decode(utf))

}

func Hexify(bslice []byte) string {

	return hex.EncodeToString(bslice)

}

type SlotOffset uint16

type SortedSlotsOffset []SlotOffset

func (s SortedSlotsOffset) Len() int {
	return len(s)

}

func (s SortedSlotsOffset) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s SortedSlotsOffset) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func CopyMapToSortedMap[L any, T ~[]L, K uint64](d map[K]T, s map[K]T) {

	keys := Keys(s)
	n := len(keys)
	for {
		swapped := false
		for i := 1; i < n; i++ {

			if keys[i] < keys[i-1] {
				keys[i-1], keys[i] = keys[i], keys[i-1]
				swapped = true
			}

		}
		if !swapped {
			break
		}

	}
	for _, k := range keys {
		d[k] = s[k]
	}

}

func Reverse(bslice []byte) []byte {
	reversedslice := make([]byte, len(bslice))
	for i := 0; i < len(bslice); i++ {
		reversedslice[i] = bslice[len(bslice)-i-1]
	}
	return reversedslice
}

func FilterMap[L any, T ~[]L, K comparable](s map[K]T, f func(L) bool) map[K]T {
	r := map[K]T{}
	for k, Vs := range s {
		for _, v := range Vs {
			if f(v) {
				r[k] = append(r[k], v)
			}
		}
	}
	return r
}

func FilterMapToList[L any, T ~[]L, K comparable](s map[K]T, f func(L) bool) T {
	r := T{}
	for _, Vs := range s {
		for _, v := range Vs {
			if f(v) {
				r = append(r, v)
			}
		}
	}
	return r
}

func FilterToMap[T any, K comparable](s []T, f func(T) (bool, K)) map[K]T {
	r := map[K]T{}
	for _, v := range s {
		status, id := f(v)
		if status {
			r[id] = v
		}
	}
	return r
}

func Filter[T any](s []T, f func(T) bool) []T {
	var r []T
	for _, v := range s {
		if f(v) {
			r = append(r, v)
		}
	}
	return r
}

func Values[M ~map[K]V, K comparable, V any](m M) []V {
	r := make([]V, 0, len(m)) // allocate memory
	for _, v := range m {
		r = append(r, v)
	}
	return r
}

func Keys[M ~map[K]V, K comparable, V any](m M) []K {
	keys := make([]K, 0, len(m)) // allocate memory
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func SplitPath(inputfile string) (string, string) {
	basepath, filename := filepath.Split(inputfile)
	if filepath.IsAbs(basepath) {
		//remove C:\\ and last \\
		basepath = basepath[3 : len(basepath)-1]
	}
	return basepath, filename
}

func HasVarLengthCols(flag uint8) bool {
	return flag&32 == 32
}

func HasNullBitmap(flag uint8) bool {
	return flag&16 == 16
}

func FindValueInStruct(colName string, v interface{}) []byte {
	structValPtr := reflect.ValueOf(v)
	//structType := reflect.TypeOf(v)
	colName = strings.ToUpper(string(colName[0])) + colName[1:]
	res := structValPtr.Elem().FieldByName(colName)
	buf := new(bytes.Buffer)
	switch res.Kind() {
	case reflect.Uint8:

		binary.Write(buf, binary.LittleEndian, uint8(res.Uint()))

	case reflect.Uint16:
		binary.Write(buf, binary.LittleEndian, uint16(res.Uint()))

	case reflect.Uint32:
		binary.Write(buf, binary.LittleEndian, uint32(res.Uint()))
	case reflect.Uint64:
		binary.Write(buf, binary.LittleEndian, res.Uint())

	case reflect.Int8:
		binary.Write(buf, binary.LittleEndian, int8(res.Int()))
	case reflect.Int16:
		binary.Write(buf, binary.LittleEndian, int16(res.Int()))
	case reflect.Int32:
		binary.Write(buf, binary.LittleEndian, int32(res.Int()))
	case reflect.Int64:
		binary.Write(buf, binary.LittleEndian, res.Int())
	}
	return buf.Bytes()
}

func checkBounds(name string, diffSize int) error {
	if diffSize > 0 {
		msg := fmt.Sprintf("datarow available size exceed at %s by %d", name, diffSize)
		mslogger.Mslogger.Error(msg)
		return errors.New(msg)
	} else {
		return nil
	}
}

func Unmarshal(data []byte, v interface{}) (int, error) {
	idx := 0
	structValPtr := reflect.ValueOf(v)
	structType := reflect.TypeOf(v)
	interfaceName := structType.Elem().Name()
	if structType.Elem().Kind() != reflect.Struct {
		return -1, errors.New("must be a struct")
	}
	for i := 0; i < structValPtr.Elem().NumField(); i++ {
		if idx >= reflect.ValueOf(data).Len() {
			mslogger.Mslogger.Error("data exhausted no remaining data to parse")
			return idx, errors.New("data exhausted no remaining data to parse")
		}
		field := structValPtr.Elem().Field(i) //StructField type
		name := structType.Elem().Field(i).Name
		switch field.Kind() {

		case reflect.Uint8:
			var temp uint8

			err := checkBounds(name, idx+1-len(data))
			if err != nil {
				return idx, err
			}

			binary.Read(bytes.NewBuffer(data[idx:idx+1]), binary.LittleEndian, &temp)
			field.SetUint(uint64(temp))
			idx += 1
		case reflect.Int16:
			var temp int16

			err := checkBounds(name, idx+2-len(data))
			if err != nil {
				return idx, err
			}

			binary.Read(bytes.NewBuffer(data[idx:idx+2]), binary.LittleEndian, &temp)
			field.SetInt(int64(temp))
			idx += 2
		case reflect.Uint16:
			var temp uint16

			err := checkBounds(name, idx+2-len(data))
			if err != nil {
				return idx, err
			}

			if name == "NumberOfVarLengthCols" &&
				!HasVarLengthCols(uint8(structValPtr.Elem().FieldByName("StatusA").Uint())) {

				return idx, nil // reached end of datarow exit
			} else {
				binary.Read(bytes.NewBuffer(data[idx:idx+2]), binary.LittleEndian, &temp)
			}

			field.SetUint(uint64(temp))
			idx += 2
		case reflect.Int32:
			var temp int32

			err := checkBounds(name, idx+4-len(data))
			if err != nil {
				return idx, err
			}

			binary.Read(bytes.NewBuffer(data[idx:idx+4]), binary.LittleEndian, &temp)
			field.SetInt(int64(temp))
			idx += 4

		case reflect.Uint32:
			var temp uint32

			err := checkBounds(name, idx+4-len(data))
			if err != nil {
				return idx, err
			}

			binary.Read(bytes.NewBuffer(data[idx:idx+4]), binary.LittleEndian, &temp)
			field.SetUint(uint64(temp))
			idx += 4

		case reflect.Uint64:
			var temp uint64

			err := checkBounds(name, idx+8-len(data))
			if err != nil {
				return idx, err
			}

			binary.Read(bytes.NewBuffer(data[idx:idx+8]), binary.LittleEndian, &temp)
			idx += 8

			field.SetUint(temp)

		case reflect.Int64:
			var temp int64

			err := checkBounds(name, idx+8-len(data))
			if err != nil {
				return idx, err
			}

			binary.Read(bytes.NewBuffer(data[idx:idx+8]), binary.LittleEndian, &temp)
			idx += 8

			field.SetInt(temp)

		case reflect.Struct:
			nameType := structType.Elem().Field(i).Type.Name()
			name := structType.Elem().Field(i).Name
			if name == "CurrentLSN" {
				continue
			}
			if nameType == "LSN" {
				var lsn LSN
				Unmarshal(data[idx:idx+10], &lsn)
				field.Set(reflect.ValueOf(lsn))
				idx += 10
			} else if nameType == "RowId" {
				var rowId RowId
				Unmarshal(data[idx:], &rowId)
				field.Set(reflect.ValueOf(rowId))
				idx += 8
			} else if nameType == "TransactionID" {
				var transID TransactionID
				Unmarshal(data[idx:], &transID)
				field.Set(reflect.ValueOf(transID))
				idx += 6
			}

		case reflect.Array:
			arrT := reflect.ArrayOf(field.Len(), reflect.TypeOf(data[0])) //create array type to hold the slice
			arr := reflect.New(arrT).Elem()                               //initialize and access array
			var end int
			if idx+field.Len() > len(data) {
				end = len(data)
			} else {
				end = idx + field.Len()
			}
			for idx, val := range data[idx:end] {

				arr.Index(idx).Set(reflect.ValueOf(val))
			}

			field.Set(arr)
			idx += field.Len()
		case reflect.Slice:
			name := structType.Elem().Field(i).Name

			if name == "FixedLenCols" {
				if interfaceName == "DataRow" {
					var dst []byte

					nofColsOffset := structValPtr.Elem().FieldByName("NofColsOffset").Uint()
					if nofColsOffset == 0 {
						mslogger.Mslogger.Error("datarow does not have fixed len cols.")
						return idx, errors.New("datarow does not have fixed len cols")
					} else if nofColsOffset < 4 {
						mslogger.Mslogger.Error(fmt.Sprintf("fixed len cols offsets cannot end before 4 %d", nofColsOffset))
						return idx, errors.New("fixed len cols offsets cannot end before 4")
					} else if nofColsOffset > 8060 {
						mslogger.Mslogger.Error(fmt.Sprintf("fixed len cols offset cannot exceed max page available area %d", nofColsOffset))
						return idx, errors.New("fixed len cols offset cannot exceed max page available area")
					} else if nofColsOffset > uint64(len(data)) {
						mslogger.Mslogger.Error(fmt.Sprintf("fixed len cols offset cannot exceed available len of data row %d", nofColsOffset))
						return idx, errors.New("fixed len cols offset cannot exceed available len of data row")
					}

					dst = make([]byte, nofColsOffset-uint64(idx))
					copy(dst, data[idx:nofColsOffset])

					field.Set(reflect.ValueOf(dst))
					idx += field.Len()
				} else if interfaceName == "IndexRow" { //already copied
					idx += field.Len()
				}

			} else if name == "NullBitmap" {
				nofCols := structValPtr.Elem().FieldByName("NumberOfCols").Uint()
				bytesNeeded := int(math.Ceil(float64(nofCols) / 8))
				if idx+bytesNeeded > len(data) {
					msg := fmt.Sprintf("Null Bitmap length exceeds available length of data by %d! \n",
						idx+bytesNeeded-len(data))
					mslogger.Mslogger.Error(msg)
					return idx, errors.New("null bitmap length exceeds available length of data ")
				}
				byteArrayDst := make([]byte, bytesNeeded)
				copy(byteArrayDst, data[idx:idx+bytesNeeded])

				field.Set(reflect.ValueOf(byteArrayDst))
				idx += bytesNeeded
			} else if name == "VarLengthColOffsets" {
				var temp int16
				var arr []int16
				nofVarLenCols := structValPtr.Elem().FieldByName("NumberOfVarLengthCols").Uint()
				for colId := 0; colId < int(nofVarLenCols); colId++ {
					if idx+2 > len(data) {
						msg := fmt.Sprintf("Var Length Col offset calculation exceeds length of data by %d!\n",
							idx+2-len(data))
						mslogger.Mslogger.Error(msg)
						return idx + 2, errors.New("var Length col offset calculation exceeds length of data")
					}
					binary.Read(bytes.NewBuffer(data[idx:idx+2]), binary.LittleEndian, &temp)
					arr = append(arr, temp)
					idx += 2
				}
				field.Set(reflect.ValueOf(arr))
			} else if name == "RowLogContentOffsets" {
				var temp uint16
				var arr []uint16
				nofCols := structValPtr.Elem().FieldByName("NumElements").Uint()
				for colId := 0; colId < int(nofCols); colId++ {
					if idx+2 > len(data) {
						msg := fmt.Sprintf("Log len col offset calculation exceeds length of data by %d!\n",
							idx+2-len(data))
						mslogger.Mslogger.Error(msg)
						return idx + 2, errors.New("log length col offset calculation exceeds length of data")
					}
					binary.Read(bytes.NewBuffer(data[idx:idx+2]), binary.LittleEndian, &temp)
					arr = append(arr, temp)
					idx += 2
				}
				field.Set(reflect.ValueOf(arr))
			}
		}

	}
	return idx, nil
}
