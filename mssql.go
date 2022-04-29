package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
)

type ColData []byte

type Row struct {
	StatusA               uint8  //1
	StatusB               uint8  //2
	NofColsOffset         uint16 //3-4
	ID                    []byte //5
	NofCols               uint16
	NullBitmap            uint8
	NumberOfVarLengthCols uint16
	ColOffsets            []uint16 //offset where data for varying length column ends
	ColsData              []ColData
}

func Unmarshal(data []byte, v interface{}) error {
	idx := 0
	structValPtr := reflect.ValueOf(v)
	structType := reflect.TypeOf(v)
	if structType.Elem().Kind() != reflect.Struct {
		return errors.New("must be a struct")
	}
	for i := 0; i < structValPtr.Elem().NumField(); i++ {
		field := structValPtr.Elem().Field(i) //StructField type
		switch field.Kind() {

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

		case reflect.Array:
			idx += field.Len()
		case reflect.Slice:
			name := structType.Elem().Field(i).Name
			if name == "ID" {
				len := structValPtr.Elem().FieldByName("NofColsOffset").Uint()

				field.SetBytes(data[idx:len])
				idx += field.Len()
			} else if name == "ColOffsets" {
				colIdx := 0
				var temp uint16
				nofCols := int(structValPtr.Elem().FieldByName("NumberOfVarLengthCols").Uint())
				colsOffsets := reflect.ValueOf(make([]uint16, 0))

				for colIdx < nofCols { // loop over varlen cols
					binary.Read(bytes.NewBuffer(data[idx:idx+2]), binary.LittleEndian, &temp)
					colsOffsets = reflect.Append(colsOffsets, reflect.ValueOf(temp))
					colIdx++
					idx += 2
				}
				field.Set(reflect.Value(colsOffsets))
			} else if name == "ColsData" {
				colIdx := 0
				nofCols := int(structValPtr.Elem().FieldByName("NumberOfVarLengthCols").Uint())
				colsData := reflect.ValueOf(make([]ColData, 0)) //allocate
				for colIdx < nofCols {
					// get value from slice
					endingOffset := int(structValPtr.Elem().FieldByName("ColOffsets").Slice(colIdx, colIdx+1).Interface().([]uint16)[0])
					colsData = reflect.Append(colsData, reflect.ValueOf(data[idx:endingOffset]))
					colIdx++

				}
				field.Set(colsData)
			}
		case reflect.Pointer:

		}

	}
	return nil
}

func main() {
	file, err := os.Open(os.Args[1]) //

	if err != nil {
		// handle the error here
		fmt.Printf("err %s for reading the MFT ", err)
		return
	}

	fsize, err := file.Stat() //file descriptor
	if err != nil {
		return
	}
	// read the file

	defer file.Close()

	bs := make([]byte, 39) //byte array to hold MFT entries

	for i := 0; i <= int(fsize.Size()); i += 39 {
		_, err := file.ReadAt(bs, int64(i))
		// fmt.Printf("\n I read %s and out is %d\n",hex.Dump(bs[20:22]), readEndian(bs[20:22]).(uint16))
		if err != nil {
			fmt.Printf("error reading file --->%s", err)
			return
		}

		var row Row
		Unmarshal(bs, &row)
		fmt.Printf("%d", row.ColsData[0])
	}

}
