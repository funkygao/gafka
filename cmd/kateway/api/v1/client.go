package api

import (
	"net"
	"net/http"
)

type Client struct {
	cf *Config

	pubConn *http.Client
	subConn *http.Client
}

// NewClient will create a PubSub client.
func NewClient(cf *Config) *Client {
	return &Client{
		cf: cf,
		pubConn: &http.Client{
			Timeout: cf.Timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 1,
				Proxy:               http.ProxyFromEnvironment,
				Dial: (&net.Dialer{
					Timeout: cf.Timeout,
				}).Dial,
				DisableKeepAlives:     false, // enable http conn reuse
				ResponseHeaderTimeout: cf.Timeout,
				TLSHandshakeTimeout:   cf.Timeout,
			},
		},
		subConn: &http.Client{
			Timeout: cf.Timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 1,
				Proxy:               http.ProxyFromEnvironment,
				Dial: (&net.Dialer{
					Timeout: cf.Timeout,
				}).Dial,
				DisableKeepAlives:     false, // enable http conn reuse
				ResponseHeaderTimeout: cf.Timeout,
				TLSHandshakeTimeout:   cf.Timeout,
			},
		},
	}
}

func (this *Client) Close() {
	//	this.conn.Transport.RoundTrip().Close() TODO
}
