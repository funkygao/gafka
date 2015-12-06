// +build go1.3

package mpool

import (
	"bytes"
	"sync"
)

var (
	bsb sync.Pool
	bs  sync.Pool
)

func init() {
	bsb.New = func() interface{} {
		return &bytes.Buffer{}
	}

	bs.New = func() interface{} {
		b := make([]byte, 32<<10) // FIXME what if Pub > 32K
		return &b
	}
}

func BytesBufferGet() *bytes.Buffer {
	return bsb.Get().(*bytes.Buffer)
}

func BytesBufferPut(b *bytes.Buffer) {
	bsb.Put(b)
}

func BytesGet() *[]byte {
	return bs.Get().(*[]byte)
}

func BytesPut(b *[]byte) {
	bs.Put(b)
}
