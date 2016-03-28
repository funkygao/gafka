package api

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"

	"github.com/funkygao/gafka/mpool"
	"github.com/funkygao/gorequest"
)

var (
	ErrSubStop     = errors.New("sub stopped")
	ErrInvalidBury = errors.New("invalid bury name")
)

const (
	ShadowRetry = "retry"
	ShadowDead  = "dead"
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

// Pub publish a keyed message to specified versioned topic.
func (this *Client) Pub(topic, ver string, key string, msg []byte) (err error) {
	buf := mpool.BytesBufferGet()
	defer mpool.BytesBufferPut(buf)

	buf.Reset()
	buf.Write(msg)

	var req *http.Request
	var u url.URL
	u.Scheme = this.cf.Pub.Scheme
	u.Host = this.cf.Pub.Endpoint
	u.Path = fmt.Sprintf("/topics/%s/%s", topic, ver)
	q := u.Query()
	q.Set("key", key)
	u.RawQuery = q.Encode()

	req, err = http.NewRequest("POST", u.String(), buf)
	if err != nil {
		return
	}

	req.Header.Set("AppId", this.cf.AppId)
	req.Header.Set("Pubkey", this.cf.Secret)

	var response *http.Response
	response, err = this.pubConn.Do(req)
	if err != nil {
		return
	}

	// TODO if 201, needn't read body
	var b []byte
	b, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}

	// reuse the connection
	response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		return errors.New(string(b))
	}

	if this.cf.Debug {
		log.Printf("--> [%s]", response.Status)
		log.Printf("Partition:%s Offset:%s",
			response.Header.Get("X-Partition"),
			response.Header.Get("X-Offset"))
	}

	return nil
}

type SubHandler func(statusCode int, msg []byte) error

func (this *Client) Sub(appid, topic, ver, group string, h SubHandler) error {
	var u url.URL
	u.Scheme = this.cf.Sub.Scheme
	u.Host = this.cf.Sub.Endpoint
	u.Path = fmt.Sprintf("/topics/%s/%s/%s", appid, topic, ver)
	q := u.Query()
	q.Set("group", group)
	u.RawQuery = q.Encode()
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}

	req.Header.Set("AppId", this.cf.AppId)
	req.Header.Set("Subkey", this.cf.Secret)
	for {
		response, err := this.subConn.Do(req)
		if err != nil {
			return err
		}

		b, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}

		if this.cf.Debug {
			log.Printf("--> [%s]", response.Status)
			log.Printf("Partition:%s Offset:%s",
				response.Header.Get("X-Partition"),
				response.Header.Get("X-Offset"))
		}

		// reuse the connection
		response.Body.Close()

		if err = h(response.StatusCode, b); err != nil {
			return err
		}
	}

}

type SubXHandler func(statusCode int, msg []byte, r *SubXResult) error
type SubXResult struct {
	Bury string
}

func (this *SubXResult) Reset() {
	this.Bury = ""
}

// SubX is advanced Sub with features of delayed ack and shadow bury.
func (this *Client) SubX(appid, topic, ver, group string, guard string, h SubXHandler) error {
	var u url.URL
	u.Scheme = this.cf.Sub.Scheme
	u.Host = this.cf.Sub.Endpoint
	u.Path = fmt.Sprintf("/topics/%s/%s/%s", appid, topic, ver)
	q := u.Query()
	q.Set("group", group)
	q.Set("ack", "1")
	if guard != "" {
		q.Set("use", guard)
	}
	u.RawQuery = q.Encode()

	req := gorequest.New()
	req.Get(u.String()).Set("AppId", this.cf.AppId).Set("Subkey", this.cf.Secret)
	r := &SubXResult{}
	for {
		response, b, errs := req.EndBytes()
		if len(errs) > 0 {
			return errs[0]
		}

		// reset the request header
		req.Set("X-Partition", "")
		req.Set("X-Offset", "")
		req.Set("X-Bury", "")

		if this.cf.Debug {
			log.Printf("--> [%s]", response.Status)
			log.Printf("Partition:%s Offset:%s",
				response.Header.Get("X-Partition"),
				response.Header.Get("X-Offset"))
		}

		r.Reset()
		if err := h(response.StatusCode, b, r); err != nil {
			return err
		}

		req.Set("X-Partition", response.Header.Get("X-Partition"))
		req.Set("X-Offset", response.Header.Get("X-Offset"))

		if r.Bury != "" {
			if r.Bury != ShadowRetry && r.Bury != ShadowDead {
				return ErrInvalidBury
			}

			req.Set("X-Bury", r.Bury)
		}
	}

}
