package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/funkygao/golib/pool"
)

var (
	port int
	mode string

	req, _ = http.NewRequest("GET", "http://localhost:9080", nil)

	hpool = newHttpPool()
)

func init() {
	flag.IntVar(&port, "p", 9090, "http port to bind")
	flag.StringVar(&mode, "mode", "standalone", "<standalone|proxy>")
	flag.Parse()
}

type httpClient struct {
	id   uint64
	pool *httpPool

	*http.Client
}

func (this *httpClient) Close() {
}

func (this *httpClient) Id() uint64 {
	return this.id
}

func (this *httpClient) IsOpen() bool {
	return true
}

func (this *httpClient) Recycle() {
	this.pool.pool.Put(this)
}

type httpPool struct {
	pool   *pool.ResourcePool
	nextId uint64
}

func newHttpPool() *httpPool {
	this := &httpPool{}

	factory := func() (pool.Resource, error) {
		conn := &httpClient{
			pool: this,
			id:   atomic.AddUint64(&this.nextId, 1),
		}

		timeout := 3 * time.Second
		conn.Client = &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				Dial: (&net.Dialer{
					Timeout:   timeout,
					KeepAlive: 60 * time.Second,
				}).Dial,
				TLSHandshakeTimeout: timeout,
			},
		}

		return conn, nil
	}

	this.pool = pool.NewResourcePool("kafka", factory,
		1000, 1000, 0, time.Second*10, time.Minute) // TODO

	return this
}

func (this *httpPool) Close() {
	this.pool.Close()
}

func (this *httpPool) Stop() {
	this.Close()
}

func (this *httpPool) Get() (*httpClient, error) {
	k, err := this.pool.Get()
	if err != nil {
		return nil, err
	}

	return k.(*httpClient), nil
}

func hello(rw http.ResponseWriter, r *http.Request) {
	io.WriteString(rw, "hello world")
	if mode == "standalone" {
		return
	}

	// proxy mode, http GET upstream
	client, err := hpool.Get()
	if err != nil {
		if client != nil {
			client.Recycle()
		}
		fmt.Println(err)
		return
	}

	response, err := client.Do(req)
	if err == nil {
		_, e := ioutil.ReadAll(response.Body)
		if e != nil {
			fmt.Println(e)
		}
		response.Body.Close() // reuse the connection
	} else {
		fmt.Println(err)
	}
	client.Recycle()
}

func main() {
	syscall.Dup2(1, 2)

	http.HandleFunc("/", hello)
	listen := fmt.Sprintf(":%d", port)
	fmt.Printf("listening on %s\n", listen)
	if err := http.ListenAndServe(listen, nil); err != nil {
		panic(err)
	}
}
