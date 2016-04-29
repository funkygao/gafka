package api

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/funkygao/gafka/mpool"
)

type PubOption struct {
	Topic, Ver string
	Async      bool
	AckAll     bool
}

// Pub publish a keyed message to specified versioned topic.
func (this *Client) Pub(key string, msg []byte, opt PubOption) (err error) {
	buf := mpool.BytesBufferGet()
	defer mpool.BytesBufferPut(buf)

	buf.Reset()
	buf.Write(msg)

	var req *http.Request
	var u url.URL
	u.Scheme = this.cf.Pub.Scheme
	u.Host = this.cf.Pub.Endpoint
	u.Path = fmt.Sprintf("/v1/msgs/%s/%s", opt.Topic, opt.Ver)
	q := u.Query()
	q.Set("key", key)
	if opt.AckAll {
		q.Set("ack", "all")
	}
	if opt.Async {
		q.Set("async", "1")
	}
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
