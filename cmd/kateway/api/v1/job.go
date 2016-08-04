package api

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/funkygao/gafka/cmd/kateway/gateway"
	"github.com/funkygao/gafka/mpool"
)

func (this *Client) AddJob(payload []byte, delay string, opt PubOption) (jobId string, err error) {
	buf := mpool.BytesBufferGet()
	defer mpool.BytesBufferPut(buf)

	buf.Reset()
	buf.Write(payload)

	var req *http.Request
	var u url.URL
	u.Scheme = this.cf.Pub.Scheme
	u.Host = this.cf.Pub.Endpoint
	u.Path = fmt.Sprintf("/v1/jobs/%s/%s", opt.Topic, opt.Ver)
	q := u.Query()
	q.Set("delay", delay)
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
		return "", errors.New(string(b))
	}

	jobId = response.Header.Get(gateway.HttpHeaderJobId)

	if this.cf.Debug {
		log.Printf("--> [%s]", response.Status)
		log.Printf("JobId:%s", response.Header.Get(gateway.HttpHeaderJobId))
	}

	return
}

func (this *Client) DeleteJob(jobId string, opt PubOption) (err error) {
	var req *http.Request
	var u url.URL
	u.Scheme = this.cf.Pub.Scheme
	u.Host = this.cf.Pub.Endpoint
	u.Path = fmt.Sprintf("/v1/jobs/%s/%s", opt.Topic, opt.Ver)
	q := u.Query()
	q.Set("id", jobId)
	u.RawQuery = q.Encode()

	req, err = http.NewRequest("DELETE", u.String(), nil)
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

	var b []byte
	b, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}

	// reuse the connection
	response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return errors.New(string(b))
	}

	return nil
}
