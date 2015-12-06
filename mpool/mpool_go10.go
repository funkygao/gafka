// +build !go1.3

package mpool

import (
	"bytes"
)

func BytesBufferGet() *bytes.Buffer {
	return &bytes.Buffer{}
}

func BytesBufferPut(b *bytes.Buffer) {}

func BytesGet() *[]byte {
	b := make([]byte, 32<<10) // FIXME what if Pub > 32K
	return &b
}

func BytesPut(b *[]byte) {}
