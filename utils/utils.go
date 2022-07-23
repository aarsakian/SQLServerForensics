package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"unicode/utf16"
	"unicode/utf8"
)

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

func ToInt(data []byte) int {
	var temp int64
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &temp)
	return int(temp)
}

type SlotOffset uint16

type SortedSlotsOffset []SlotOffset

func (s SortedSlotsOffset) Len() int {
	return len(s)

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

func FilterMap[T any, K comparable](s map[K][]T, f func(T) bool) map[K][]T {
	r := map[K][]T{}
	for k, Vs := range s {
		for _, v := range Vs {
			if f(v) {
				r[k] = append(r[k], v)
			}
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
			name := structType.Elem().Field(i).Name
			if name == "NullBitmap" {
				nofCols := structValPtr.Elem().FieldByName("NumberOfCols").Uint()
				bytesNeeded := int(math.Ceil(float64(nofCols) / 8))
				binary.Read(bytes.NewBuffer(data[idx:idx+bytesNeeded]), binary.LittleEndian, &temp)
				field.SetUint(uint64(temp))
				idx += bytesNeeded
			} else {
				binary.Read(bytes.NewBuffer(data[idx:idx+2]), binary.LittleEndian, &temp)
				field.SetUint(uint64(temp))
				idx += 2
			}
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
			for idx, val := range data[idx : idx+field.Len()] {
				arr.Index(idx).Set(reflect.ValueOf(val))
			}

			field.Set(arr)
			idx += field.Len()
		case reflect.Slice:
			name := structType.Elem().Field(i).Name
			if name == "FixedLenCols" {

				nofColsOffset := structValPtr.Elem().FieldByName("NofColsOffset").Uint()

				field.Set(reflect.ValueOf(data[idx:nofColsOffset]))
				idx += field.Len()

			} else if name == "VarLengthColOffsets" {
				var temp uint16
				var arr []uint16
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
