package utils

import "fmt"

type SqlVariantProperties struct {
	Precision     uint8
	Scale         uint8
	MaximumLength uint16
	CollationId   uint32
}

type SqlVariant struct {
	BaseType   uint8
	Version    uint8
	Properties *SqlVariantProperties
	Value      []byte
}

func (sqlVariant *SqlVariant) Parse(data []byte) {

	Unmarshal(data, sqlVariant)
	var sqlVariantProperties SqlVariantProperties
	switch sqlVariant.BaseType {
	case 0x38: //int
		sqlVariantProperties = SqlVariantProperties{Precision: data[2], Scale: data[3]}
		sqlVariant.Value = append([]byte(nil), data[3:]...)
	case 0x23: //string

		sqlVariantProperties = SqlVariantProperties{MaximumLength: ToUint16(data[2:4]),
			CollationId: ToUint32(data[4:8])}
		sqlVariant.Value = data[8:]
	}
	sqlVariant.Properties = &sqlVariantProperties

}

func (sqlVariant SqlVariant) GetData() string {
	switch sqlVariant.BaseType {
	case 0x23:
		return fmt.Sprintf("%d", ToInt32(sqlVariant.Value))
	case 0x7f:
		return fmt.Sprintf("%d", ToInt64(sqlVariant.Value))
	case 0xad: //string
		return fmt.Sprintf("%x", sqlVariant.Value)
	}
	return ""
}
