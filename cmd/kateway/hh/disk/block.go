package disk

type block struct {
	key   string
	value []byte
}

func (b *block) size() int64 {
	return int64(len(b.key) + len(b.value) + 8)
}

func (b *block) keyLen() uint32 {
	return uint32(len(b.key))
}

func (b *block) valueLen() uint32 {
	return uint32(len(b.value))
}
