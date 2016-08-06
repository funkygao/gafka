package mpool

import (
	"testing"
)

func BenchmarkIntern(b *testing.B) {
	i := NewIntern()
	b.RunParallel(func(pb *testing.PB) {
		var s string

		for pb.Next() {
			s = i.String("app1" + "." + "order" + "." + "v1")
		}

		_ = s
	})
}
