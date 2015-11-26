package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/funkygao/golib/stress"
)

func main() {
	flag.Parse()

	http.DefaultClient.Timeout = time.Second * 30
	stress.RunStress(pubLoop)
}

func pubLoop(seq int) {
	timeout := 3 * time.Second
	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: 60 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: timeout,
		},
	}

	for i := 0; i < 1000; i++ {
		pub(seq, i, httpClient)
	}
}

func pub(seq int, n int, httpClient *http.Client) {
	req, err := http.NewRequest("POST",
		"http://localhost:9090/v1/topics/foobar?ack=2&timeout=1&retry=3",
		bytes.NewBuffer([]byte("m=hello world")))
	if err != nil {
		log.Fatalf("Error Occured. %+v", err)
		stress.IncCounter("fail", 1)
		return
	}
	req.Header.Set("Pubkey", "mypubkey")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// use httpClient to send request
	response, err := httpClient.Do(req)
	if err != nil && response == nil {
		stress.IncCounter("fail", 1)
		log.Printf("Error sending request to API endpoint. %+v\n", err)
	} else {
		// Close the connection to reuse it
		defer response.Body.Close()

		if response.StatusCode != http.StatusOK {
			stress.IncCounter("fail", 1)
			return
		}

		// Let's check if the work actually is done
		// We have seen inconsistencies even when we get 200 OK response
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatalf("Couldn't parse response body. %+v", err)
		}

		stress.IncCounter("success", 1)

		if false {
			log.Println("Response Body:", string(body))
		}
	}

}
