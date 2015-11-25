package main

import (
	"net"
	"net/http"

	"github.com/funkygao/golib/stress"
)

func main() {
	stress.RunStress(pub)
}

func pub(seq int) {
	to := 3
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   to * time.Second,
				KeepAlive: 60 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: to * time.Second,
		},
	}
	client.Get(url)
}
