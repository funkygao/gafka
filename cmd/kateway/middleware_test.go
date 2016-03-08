package main

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/funkygao/gafka/mpool"
)

// 764 ns/op 96 B/op 4 allocs/op
func BenchmarkBuildCommonLogLine(b *testing.B) {
	gw := &Gateway{}
	httpReqRaw := strings.TrimSpace(fmt.Sprintf(`
POST /topics/foobar/v1 HTTP/1.1
Host: localhost:9191
User-Agent: Go-http-client/1.1
Content-Length: %d
Content-Type: application/x-www-form-urlencoded
Appid: myappid
Pubkey: mypubkey
Accept-Encoding: gzip`, 100)) + "\r\n\r\n"

	r, err := http.ReadRequest(bufio.NewReader(strings.NewReader(httpReqRaw)))
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
