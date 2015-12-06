// +build !go1.3

package mpool

import (
	"bytes"
)

func BytesBufferGet() *bytes.Buffer {
	return &bytes.Buffer{}
}

func BytesBufferPut(b *bytes.Buffer) {}
