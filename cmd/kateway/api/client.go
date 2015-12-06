package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"

	"github.com/funkygao/gafka/mpool"
)

type SubHandler func(statusCode int, msg []byte) error

type pubResponse struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:offset`
}

type Client struct {
	cf *Config

	conn *http.Client
	addr string
}

func NewClient(appId string, cf *Config) *Client {
	if cf == nil {
		cf = DefaultConfig()
	}
	cf.AppId = appId
	return &Client{
		cf: cf,
	}
}

func (this *Client) Connect(addr string) {
	this.addr = addr
	this.conn = &http.Client{
		Timeout: this.cf.Timeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1,
			Proxy:               http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout: this.cf.Timeout,
				//KeepAlive: this.cf.KeepAlive, TODO
			}).Dial,
			DisableKeepAlives:     false, // enable http conn reuse
			ResponseHeaderTimeout: this.cf.Timeout,
			TLSHandshakeTimeout:   this.cf.Timeout,
		},
	}
}

func (this *Client) Close() {
	//	this.conn.Transport.RoundTrip().Close() TODO
}

// TODO async
func (this *Client) Publish(ver, topic, key string, msg []byte) (partition int,
	offset int64, err error) {
	buf := mpool.BytesBufferGet()
	defer mpool.BytesBufferPut(buf)

	buf.Reset()
	buf.Write(msg)
	var req *http.Request
	url := fmt.Sprintf("%s/topics/%s/%s?key=%s", this.addr, ver, topic, key)
	req, err = http.NewRequest("POST", url, buf)
	if err != nil {
		return
	}

	if this.cf.Debug {
		log.Printf("pub: %s", url)
	}

	req.Header.Set("AppId", this.cf.AppId)
	req.Header.Set("Pubkey", this.cf.Secret)

	var response *http.Response
	response, err = this.conn.Do(req)
	if err != nil {
		return
	}

	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}

	// reuse the connection
	response.Body.Close()

	if this.cf.Debug {
		log.Printf("got: %s", string(b))
	}
	var v pubResponse
	err = json.Unmarshal(b, &v)
	if err != nil {
		return
	}

	return v.Partition, v.Offset, nil
}

func (this *Client) Subscribe(ver, topic, group string, h SubHandler) error {
	url := fmt.Sprintf("%s/topics/%s/%s/%s?limit=", this.addr, ver, topic, group)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("AppId", this.cf.AppId)
	req.Header.Set("Subkey", this.cf.Secret)
	for {
		if this.cf.Debug {
			log.Printf("sub: %s", url)
		}

		response, err := this.conn.Do(req)
		if err != nil {
			return err
		}

		b, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}

		if this.cf.Debug {
			log.Printf("got: [%s] %s", response.Status, string(b))
		}

		// reuse the connection
		response.Body.Close()

		if err = h(response.StatusCode, b); err != nil {
			return err
		}
	}

}
