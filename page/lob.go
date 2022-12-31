package page

// large objects storage
type LOB struct {
	Unknown  [2]byte
	Length   uint16
	Id       uint32
	Unknown2 [4]byte
	Type     uint16
	Content  []byte
}
