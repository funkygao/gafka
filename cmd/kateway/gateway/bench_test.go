// +build !fasthttp

package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	glog "log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/funkygao/gafka"
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

func BenchmarkStrconvItoa(b *testing.B) {
	for i := 0; i < b.N; i++ {
		strconv.Itoa(100000)
	}
}

func newGatewayForTest(b *testing.B, store string) *Gateway {
	options.Zone = "local"
	options.PubHttpAddr = ":9191"
	options.SubHttpAddr = ":9192"
	options.Store = store
	options.Debug = false
	options.DisableMetrics = false

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

	req, err := mockHttpRequest()
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
	store.DefaultPubStore = kafka.NewPubStore(100, 5, 0, &wg, false, true)
	store.DefaultPubStore.Start()

	data := []byte(strings.Repeat("X", msgSize))
	for i := 0; i < b.N; i++ {
		store.DefaultPubStore.SyncPub("me", "foobar", nil, data)
	}
}

func BenchmarkKatewayPubKafka(b *testing.B) {
	b.Skip("skipped")

	runBenchmarkPub(b, "kafka", 1<<10)
}

func BenchmarkKatewayPubDummy1K(b *testing.B) {
	runBenchmarkPub(b, "dummy", 1<<10)
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
	type pubResponse struct {
		Partition int32 `json:"partition"`
		Offset    int64 `json:"offset"`
	}

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
		_ = fmt.Sprintf(`{"partition":%d,"offset:%d"}`, 5, 32)
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

// 1k log line
// on the vm, go1.5
// 12614 ns/op	81.18 MB/s	3680 B/op  5 allocs/op
// 85k line/second
// on the physical server, go1.4
// 9944 ns/op	102.97 MB/ 3714 B/op   7 allocs/op
// 100k line/second
//
// 0.5k log line
// on the vm, go1.5
// 8111 ns/op	61.64 MB/s	 528 B/op  2 allocs/op
// 135k line/second
// on the physical server, go1.4
// 4677 ns/op	106.89 MB/s	 547 B/op  4 allocs/op
// 200k line/second
func BenchmarkLogAppend(b *testing.B) {
	sz := 1 << 10
	line := strings.Repeat("X", sz)
	f, err := os.OpenFile("log.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}

	l := glog.New(f, "", glog.LstdFlags)
	for i := 0; i < b.N; i++ {
		l.Println(line)
	}
	b.SetBytes(int64(sz))
	os.Remove("log.log")
}
