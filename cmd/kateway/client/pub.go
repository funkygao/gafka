package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"
)

var (
	addr   string
	msgKey string
	n      int
)

func init() {
	flag.StringVar(&addr, "addr", "http://localhost:9191", "pub kateway addr")
	flag.IntVar(&n, "n", 50, "run pub how many times")
	flag.StringVar(&msgKey, "key", "", "pub message key")

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

	var b bytes.Buffer
	for i := 0; i < n; i++ {
		b.Reset()
		b.WriteString(fmt.Sprintf("hello@%s  %d", time.Now(), i))
		req, _ := http.NewRequest("POST",
			fmt.Sprintf("%s/v1/topics/foobar?key=%s", addr, msgKey),
			strings.NewReader(b.String()))
		req.Header.Set("Pubkey", "mypubkey")
		response, err := client.Do(req)
		if err != nil {
			panic(err)
		}

		b, err := ioutil.ReadAll(response.Body)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(b))
		response.Body.Close() // reuse the connection
	}

}
