package mpool

import (
	"testing"
)

func BenchmarkBytesPoolGetThenPut(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b := BytesBufferGet()
		b.Reset()
		BytesBufferPut(b)
	}
}

func BenchmarkMessageNewAndFree(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := NewMessage(1089)
		m.Free()
	}
}
