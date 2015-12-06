package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/funkygao/gafka/ctx"
)

type neverEnding byte

var gw *Gateway

func (b neverEnding) Read(p []byte) (n int, err error) {
	if len(p) < 16 {
		for i := range p {
			p[i] = byte(b)
		}
	} else {
		b.Read(p[:len(p)/2])
		copy(p[len(p)/2:], p)
	}
	return len(p), nil
}

func BenchmarkNeverending(b *testing.B) {
	buf := make([]byte, 4096)
	A := neverEnding('A')
	for i := 0; i < b.N; i++ {
		A.Read(buf)
	}
}

func newGatewayForTest(b *testing.B, store string) *Gateway {
	options.zone = "local"
	options.cluster = "me"
	options.pubHttpAddr = ":9191"
	options.subHttpAddr = ":9192"
	options.store = store
	options.debug = false
	options.logLevel = "info"

	ctx.LoadConfig("/etc/kateway.cf")

	gw := NewGateway("1", time.Hour)
	if err := gw.Start(); err != nil {
		b.Fatal(err)
	}

	return gw
}

func runBenchmarkPub(b *testing.B, store string) {
	if gw == nil {
		gw = newGatewayForTest(b, store)
	}

	b.ReportAllocs()
	const msgSize = 1 << 10
	b.SetBytes(msgSize)
	httpReqRaw := strings.TrimSpace(fmt.Sprintf(`
POST /topics/v1/foobar HTTP/1.1
Host: localhost:9191
User-Agent: Go-http-client/1.1
Content-Length: %d
Content-Type: application/x-www-form-urlencoded
Appid: myappid
Pubkey: mypubkey
Accept-Encoding: gzip`, msgSize)) + "\r\n\r\n"

	req, err := http.ReadRequest(bufio.NewReader(strings.NewReader(httpReqRaw)))
	if err != nil {
		b.Fatal(err)
	}

	rw := httptest.NewRecorder()
	lr := io.LimitReader(neverEnding('a'), msgSize)
	body := ioutil.NopCloser(lr)

	for i := 0; i < b.N; i++ {
		rw.Body.Reset()
		lr.(*io.LimitedReader).N = msgSize
		req.Body = body
		gw.pubHandler(rw, req)
	}
}

func BenchmarkPubKafka(b *testing.B) {
	runBenchmarkPub(b, "kafka")
}

func BenchmarkPubDumb(b *testing.B) {
	runBenchmarkPub(b, "dumb")
}
