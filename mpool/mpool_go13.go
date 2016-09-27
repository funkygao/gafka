// +build go1.3

package mpool

import (
	"bytes"
	"sync"
)

var (
	bsb           sync.Pool
	accessLogPool sync.Pool
)

func init() {
	bsb.New = func() interface{} {
		return bytes.NewBuffer(make([]byte, 100))
	}

	accessLogPool.New = func() interface{} {
		return make([]byte, 0, accessLogLineMaxBytes)
	}
}

func BytesBufferGet() *bytes.Buffer {
	return bsb.Get().(*bytes.Buffer)
}

func BytesBufferPut(b *bytes.Buffer) {
	bsb.Put(b)
}

func AccessLogLineBufferGet() []byte {
	return accessLogPool.Get().([]byte)
}

func AccessLogLineBufferPut(b []byte) {
	accessLogPool.Put(b)
}
