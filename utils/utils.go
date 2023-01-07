package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

type Record []string
type Records [][]string

type LSN struct {
	P1 uint32
	P2 uint32
	P3 uint32
}

type Auid struct {
	UniqueId uint16
	ObjectId uint32
	Zeros    uint32
}

func ToStructAuid(data []byte) Auid {

	var auid Auid
	Unmarshal(data, &auid)
	return auid

}

func RemoveSignBit(val int16) int16 {
	return int16(uint16(val<<1) >> 1)
}

func ToInt64(data []byte) int {
	var temp int64
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &temp)
	return int(temp)
}

func ToInt32(data []byte) int {
	var temp int32
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &temp)
	return int(temp)
}

type SlotOffset uint16

type SortedSlotsOffset []SlotOffset

func (s SortedSlotsOffset) Len() int {
	return len(s)

}

func HasFlagSet(bitmap []byte, flagPos int, nofCols int) bool {
	var bitrepresentation string
	if len(bitmap) == 1 {

		temp := uint(bitmap[0])
		bitrepresentation = strconv.FormatUint(uint64(temp), 2)
	} else if len(bitmap) == 2 {
		var temp uint16
		binary.Read(bytes.NewBuffer(bitmap), binary.LittleEndian, &temp)
		bitrepresentation = strconv.FormatUint(uint64(temp), 2)
	} else {
		var temp uint32
		binary.Read(bytes.NewBuffer(bitmap), binary.LittleEndian, &temp)
		bitrepresentation = strconv.FormatUint(uint64(temp), 2)
	}

	for len(bitrepresentation) <= nofCols {
		bitrepresentation = "0" + bitrepresentation
	}

	bitflag := bitrepresentation[len(bitrepresentation)-flagPos]
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

func (s SortedSlotsOffset) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s SortedSlotsOffset) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
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

func Unmarshal(data []byte, v interface{}) error {
	idx := 0
	structValPtr := reflect.ValueOf(v)
	structType := reflect.TypeOf(v)
	if structType.Elem().Kind() != reflect.Struct {
		return errors.New("must be a struct")
	}
	for i := 0; i < structValPtr.Elem().NumField(); i++ {
		if idx >= reflect.ValueOf(data).Len() {
			break
		}
		field := structValPtr.Elem().Field(i) //StructField type
		switch field.Kind() {
		case reflect.String:

		case reflect.Uint8:
			var temp uint8
			binary.Read(bytes.NewBuffer(data[idx:idx+1]), binary.LittleEndian, &temp)
			field.SetUint(uint64(temp))
			idx += 1
		case reflect.Uint16:
			var temp uint16

			binary.Read(bytes.NewBuffer(data[idx:idx+2]), binary.LittleEndian, &temp)
			field.SetUint(uint64(temp))
			idx += 2

		case reflect.Uint32:
			var temp uint32

			binary.Read(bytes.NewBuffer(data[idx:idx+4]), binary.LittleEndian, &temp)
			field.SetUint(uint64(temp))
			idx += 4

		case reflect.Uint64:
			var temp uint64

			binary.Read(bytes.NewBuffer(data[idx:idx+8]), binary.LittleEndian, &temp)
			idx += 8

			field.SetUint(temp)

		case reflect.Int32:
			var temp int32
			binary.Read(bytes.NewBuffer(data[idx:idx+4]), binary.LittleEndian, &temp)
			field.SetInt(int64(temp))
			idx += 4
		case reflect.Struct:
			name := structType.Elem().Field(i).Name
			if name == "LSN" {
				var lsn LSN
				Unmarshal(data[idx:idx+12], &lsn)
				field.Set(reflect.ValueOf(lsn))
				idx += 12
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

				nofColsOffset := structValPtr.Elem().FieldByName("NofColsOffset").Uint()
				dst := make([]byte, nofColsOffset-uint64(idx))

				copy(dst, data[idx:nofColsOffset])
				field.Set(reflect.ValueOf(dst))
				idx += field.Len()

			} else if name == "NullBitmap" {
				nofCols := structValPtr.Elem().FieldByName("NumberOfCols").Uint()
				bytesNeeded := int(math.Ceil(float64(nofCols) / 8))
				byteArrayDst := make([]byte, bytesNeeded)
				copy(byteArrayDst, data[idx:idx+bytesNeeded])

				field.Set(reflect.ValueOf(byteArrayDst))
				idx += bytesNeeded
			} else if name == "VarLengthColOffsets" {
				var temp int16
				var arr []int16
				nofVarLenCols := structValPtr.Elem().FieldByName("NumberOfVarLengthCols").Uint()

				for colId := 0; colId < int(nofVarLenCols); colId++ {
					binary.Read(bytes.NewBuffer(data[idx:idx+2]), binary.LittleEndian, &temp)
					arr = append(arr, temp)
					idx += 2
				}
				field.Set(reflect.ValueOf(arr))
			}
		}

	}
	return nil
}
