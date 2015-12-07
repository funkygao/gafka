package mpool

import (
	"testing"
)

func BenchmarkGetThenPut(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b := BytesBufferGet()
		b.Reset()
		BytesBufferPut(b)
	}
}
