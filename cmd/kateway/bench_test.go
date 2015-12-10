package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/meta/zkmeta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/cmd/kateway/store/kafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/mpool"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
	"github.com/julienschmidt/httprouter"
)

type neverEnding byte

var gw *Gateway

func init() {
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
	options.logLevel = "info"

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

	req, err := http.ReadRequest(bufio.NewReader(strings.NewReader(httpReqRaw)))
	if err != nil {
		b.Fatal(err)
	}

	rw := httptest.NewRecorder()
	lr := io.LimitReader(neverEnding('a'), msgSize)
	body := ioutil.NopCloser(lr)

	param := httprouter.Params{
		httprouter.Param{Key: "topic", Value: "foobar"},
		httprouter.Param{Key: "ver", Value: "v1"},
	}

	for i := 0; i < b.N; i++ {
		rw.Body.Reset()
		lr.(*io.LimitedReader).N = msgSize
		req.Body = body
		gw.pubHandler(rw, req, param)
	}
}

func BenchmarkDirectKafkaProduce1K(b *testing.B) {
	b.Skip("skipped")

	msgSize := 1 << 10
	b.ReportAllocs()
	b.SetBytes(int64(msgSize))

	ctx.LoadConfig("/etc/kateway.cf")
	cf := zkmeta.DefaultConfig("local")
	cf.Refresh = time.Hour
	meta.Default = zkmeta.New(cf)
	meta.Default.Start()
	var wg sync.WaitGroup
	store.DefaultPubStore = kafka.NewPubStore(&wg, false)
	store.DefaultPubStore.Start()

	data := []byte(strings.Repeat("X", msgSize))
	for i := 0; i < b.N; i++ {
		store.DefaultPubStore.SyncPub("me", "foobar", "", data)
	}
}

func BenchmarkKatewayPubKafka(b *testing.B) {
	b.Skip("skipped")

	runBenchmarkPub(b, "kafka", 1<<10)
}

func BenchmarkKatewayPubDumb1K(b *testing.B) {
	runBenchmarkPub(b, "dumb", 1<<10)
}

func BenchmarkGorillaMux(b *testing.B) {
	b.Skip("skip for now")

	router := mux.NewRouter()
	handler := func(w http.ResponseWriter, r *http.Request) {}
	router.HandleFunc("/topics/{topic}/{ver}", handler)
	router.HandleFunc("/ws/topics/{topic}/{ver}", handler)
	router.HandleFunc("/ver", handler)
	router.HandleFunc("/help", handler)
	router.HandleFunc("/stat", handler)
	router.HandleFunc("/ping", handler)
	router.HandleFunc("/clusters", handler)

	request, _ := http.NewRequest("GET", "/topics/anything/v1", nil)
	for i := 0; i < b.N; i++ {
		router.ServeHTTP(nil, request)
	}
}

func BenchmarkHttpRouter(b *testing.B) {
	b.Skip("skip for now")

	router := httprouter.New()
	handler := func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {}
	router.POST("/topics/:topic/:ver", handler)
	router.POST("/ws/topics/:topic/:ver", handler)
	router.GET("/ver", handler)
	router.GET("/help", handler)
	router.GET("/stat", handler)
	router.GET("/ping", handler)
	router.GET("/clusters", handler)

	request, _ := http.NewRequest("POST", "/topics/anything/v1", nil)
	for i := 0; i < b.N; i++ {
		router.ServeHTTP(nil, request)
	}
}

func BenchmxarkPubJsonResponse(b *testing.B) {
	response := pubResponse{
		Partition: 5,
		Offset:    32,
	}
	for i := 0; i < b.N; i++ {
		json.Marshal(response)
	}
}

func BenchmarkPubManualJsonResponse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fmt.Sprintf(`{"partition":%d,"offset:%d"}`, 5, 32)
	}
}

func BenchmarkManualCreateJson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buffer := mpool.BytesBufferGet()

		buffer.Reset()
		buffer.WriteString(`{"partition":`)
		buffer.WriteString(strconv.Itoa(int(6)))
		buffer.WriteString(`,"offset":`)
		buffer.WriteString(strconv.Itoa(int(7)))
		buffer.WriteString(`}`)

		mpool.BytesBufferPut(buffer)
	}
}
