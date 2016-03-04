// +build !go1.3

package mpool

import (
	"bytes"
)

func BytesBufferGet() *bytes.Buffer {
	return &bytes.Buffer{}
}

func BytesBufferPut(b *bytes.Buffer) {}

func AccessLogLineBufferGet() []byte {
	return make([]byte, 0, accessLogLineMaxBytes)
}

func AccessLogLineBufferPut(b []byte) {
	bytesPool.Put(b)
}
