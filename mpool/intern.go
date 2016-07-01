package mpool

import (
	"sync"
)

var (
	internPool sync.Pool = sync.Pool{
		New: func() interface{} {
			return make(map[string]string)
		},
	}
)

// String returns s, interned.
func String(s string) string {
	m := internPool.Get().(map[string]string)
	c, ok := m[s]
	if ok {
		internPool.Put(m)
		return c
	}

	m[s] = s
	internPool.Put(m)
	return s
}

// Bytes returns b converted to a string, interned.
func Bytes(b []byte) string {
	m := internPool.Get().(map[string]string)
	c, ok := m[string(b)]
	if ok {
		internPool.Put(m)
		return c
	}

	s := string(b)
	m[s] = s
	internPool.Put(m)
	return s
}
