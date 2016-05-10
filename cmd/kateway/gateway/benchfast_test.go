// +build fasthttp

package gateway

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buaazp/fasthttprouter"
	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	log "github.com/funkygao/log4go"
	"github.com/valyala/fasthttp"
)

type neverEnding byte

var gw *Gateway

func init() {
	gafka.BuildId = "test"
	log.AddFilter("stdout", log.ERROR, log.NewConsoleLogWriter())
}

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
	options.pubHttpAddr = ":9191"
	options.subHttpAddr = ":9192"
	options.store = store
	options.debug = false
	options.disableMetrics = false
	options.enableBreaker = true

	ctx.LoadConfig("/etc/kateway.cf")

	gw := NewGateway("1", time.Hour)
	if err := gw.Start(); err != nil {
		b.Fatal(err)
	}

	return gw
}

func runBenchmarkPub(b *testing.B, store string, msgSize int64) {
	if gw == nil {
		gw = newGatewayForTest(b, store)
	}

	b.ReportAllocs()

	httpReqRaw := strings.TrimSpace(fmt.Sprintf(`
POST /topics/foobar/v1 HTTP/1.1
Host: localhost:9191
User-Agent: Go-http-client/1.1
Content-Length: %d
Content-Type: application/x-www-form-urlencoded
Appid: myappid
Pubkey: mypubkey
Accept-Encoding: gzip`, msgSize)) + "\r\n\r\n"
	b.SetBytes(msgSize + int64(len(httpReqRaw)))

	param := fasthttprouter.Params{
		fasthttprouter.Param{Key: "topic", Value: "foobar"},
		fasthttprouter.Param{Key: "ver", Value: "v1"},
	}
	req := &fasthttp.Request{}
	req.SetRequestURI("/topics/foobar/v1")

	ctx := &fasthttp.RequestCtx{}
	ctx.Init(req, nil, nil)
	ctx.Request.SetBodyString(strings.Repeat("X", int(msgSize)))
	ctx.Request.Header.SetMethod("POST")

	for i := 0; i < b.N; i++ {
		ctx.Response.Reset()
		gw.pubHandler(ctx, param)
	}
}

func BenchmarkKatewayPubDummy1K(b *testing.B) {
	runBenchmarkPub(b, "dummy", 1<<10)
}
