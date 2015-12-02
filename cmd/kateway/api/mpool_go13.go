// +build go1.3

package api

import (
	"bytes"
	"sync"
)

var mpool sync.Pool

func init() {
	mpool.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

func mpoolGet() *bytes.Buffer {
	return mpool.Get().(*bytes.Buffer)
}

func mpoolPut(b *bytes.Buffer) {
	mpool.Put(b)
}
