// +build go1.3

package mpool

import (
	"bytes"
	"sync"
)

var (
	bsb sync.Pool
)

func init() {
	bsb.New = func() interface{} {
		return &bytes.Buffer{}
	}

}

func BytesBufferGet() *bytes.Buffer {
	return bsb.Get().(*bytes.Buffer)
}

func BytesBufferPut(b *bytes.Buffer) {
	bsb.Put(b)
}
