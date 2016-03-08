package main

import (
	"testing"

	"github.com/funkygao/gafka/mpool"
)

// 764 ns/op 96 B/op 4 allocs/op
func BenchmarkBuildCommonLogLine(b *testing.B) {
	gw := &Gateway{}
	r, err := buildHttpRequest()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := mpool.AccessLogLineBufferGet()[0:]
		gw.buildCommonLogLine(buf, r, 200, 100)
		mpool.AccessLogLineBufferPut(buf)
	}
}
