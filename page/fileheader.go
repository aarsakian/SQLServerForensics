package page

type FileHeader struct {
	Status    uint32
	BindingId [16]byte
	Size      uint32
}
