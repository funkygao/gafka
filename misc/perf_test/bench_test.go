package x

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
)

var (
	s    = strings.Repeat("abc", 100)
	p    []byte
	pool = sync.Pool{
		New: func() interface{} { return new(bytes.Buffer) },
	}
	p1 = bytes.Repeat([]byte{'a'}, 100)
)

func BenchmarkSliceConvert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		p = []byte(s[3:6])
	}
}

func BenchmarkConvertSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
		p = []byte(s)[3:6]
	}
}

func BenchmarkStrconv(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = strconv.Itoa(500)
	}
}

func BenchmarkSprintf(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%d", 500)
	}
}

func BenchmarkReuse(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get().(*bytes.Buffer)
			buf.Write(p1)
			_ = buf.String()
			buf.Reset()
			pool.Put(buf)
		}
	})
}

func BenchmarkNoReuse(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var buf bytes.Buffer
			buf.Write(p1)
			_ = buf.String()
		}
	})
}

func BenchmarkConvert1(b *testing.B) {
	p := bytes.Repeat([]byte{'a'}, 10)
	var n int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := string(p)
		n += len(s)
	}
	_ = n
}

var p2 = bytes.Repeat([]byte{'a'}, 100)
var m = make(map[string]bool)

func BenchmarkMapKey1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = m[string(p2)]
	}
}

func BenchmarkMapKey2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := string(p2)
		_ = m[s]
	}
}

var interned = make(map[string]string)

func intern(b []byte) string {
	s, ok := interned[string(b)] // does not allocate!
	if ok {
		return s
	}
	s = string(b)
	interned[s] = s
	return s
}

var p5 = bytes.Repeat([]byte{'a'}, 100)

func BenchmarkConvert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = string(p5)
	}
}
func BenchmarkIntern(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = intern(p)
	}
}

const size = 100

func BenchmarkDelayedAlloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var s []int
		for i := 0; i < size; i++ {
			s = append(s, i)
		}
		_ = s
	}
}

func BenchmarkOneAlloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := make([]int, 0, 100)
		for i := 0; i < size; i++ {
			s = append(s, i)
		}
		_ = s
	}
}
