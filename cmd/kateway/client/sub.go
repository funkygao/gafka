package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

var (
	addr string
	n    int
)

func init() {
	flag.StringVar(&addr, "addr", "http://localhost:9192", "sub kateway addr")
	flag.IntVar(&n, "n", 10, "run sub how many times")
	flag.Parse()

	http.DefaultClient.Timeout = time.Second * 30
}

func main() {
	timeout := 3 * time.Second
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: 60 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: timeout,
		},
	}

	for i := 0; i < n; i++ {
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s/v1/topics/foobar", addr), nil)
		req.Header.Set("Subkey", "mysubkey")
		response, err := client.Do(req)
		if err != nil {
			panic(err)
		}

		b, err := ioutil.ReadAll(response.Body)
		if err != nil {
			panic(err)
		}

		fmt.Sprintf("got: %s\n", string(b))
		response.Body.Close() // reuse the connection
	}

}
