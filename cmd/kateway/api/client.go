package api

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"

	"github.com/funkygao/gafka/mpool"
	"github.com/funkygao/gorequest"
)

type SubHandler func(statusCode int, msg []byte) error

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
func (this *Client) Publish(topic, ver, key string, msg []byte) (err error) {
	buf := mpool.BytesBufferGet()
	defer mpool.BytesBufferPut(buf)

	buf.Reset()
	buf.Write(msg)
	var req *http.Request
	url := fmt.Sprintf("%s/topics/%s/%s?key=%s", this.addr, topic, ver, key)
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

	if response.StatusCode != http.StatusCreated {
		return errors.New(string(b))
	}

	if this.cf.Debug {
		log.Printf("got: %s", string(b))
	}

	return nil
}

func (this *Client) Subscribe(appid, topic, ver, group string, h SubHandler) error {
	url := fmt.Sprintf("%s/topics/%s/%s/%s?group=%s&limit=1", this.addr,
		appid, topic, ver, group)
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

func (this *Client) AckedSubscribe(appid, topic, ver, group string, h SubHandler) error {
	url := fmt.Sprintf("%s/topics/%s/%s/%s?group=%s&ack=1", this.addr,
		appid, topic, ver, group)
	req := gorequest.New()
	req.Get(url).Set("AppId", this.cf.AppId).Set("Subkey", this.cf.Secret)
	for {
		response, b, errs := req.EndBytes()
		if len(errs) > 0 {
			return errs[0]
		}

		req.Set("X-Partition", response.Header.Get("X-Partition"))
		req.Set("X-Offset", response.Header.Get("X-Offset"))

		if err := h(response.StatusCode, b); err != nil {
			return err
		}
	}

}
