package mpool

import (
	"testing"

	log "github.com/funkygao/log4go"
)

func init() {
	log.Disable()
}

func BenchmarkBytesPoolGetThenPut(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b := BytesBufferGet()
		b.Reset()
		BytesBufferPut(b)
	}
}

func BenchmarkMessageLessThan1KNewAndFree(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := NewMessage(987)
		m.Free()
	}
}

func BenchmarkMessageGreaterThan1KNewAndFree(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := NewMessage(1089)
		m.Free()
	}
}

func BenchmarkMessageGreaterThan2KNewAndFree(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := NewMessage(3 << 10)
		m.Free()
	}
}

func BenchmarkMessageGreaterThan8KNewAndFree(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := NewMessage(10 << 10)
		m.Free()
	}
}

func BenchmarkMessageGreaterThan64KNewAndFree(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := NewMessage(78 << 10)
		m.Free()
	}
}

func BenchmarkMessageGreaterThan256KNewAndFree(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := NewMessage(2 << 20)
		m.Free()
	}
}
