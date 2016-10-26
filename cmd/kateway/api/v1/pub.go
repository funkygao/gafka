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

type PubOption struct {
	Topic, Ver string
	Async      bool
	AckAll     bool
	Tag        string
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

	req.Header.Set(gateway.HttpHeaderAppid, this.cf.AppId)
	req.Header.Set(gateway.HttpHeaderPubkey, this.cf.Secret)
	if opt.Tag != "" {
		req.Header.Set(gateway.HttpHeaderMsgTag, opt.Tag)
	}

	var response *http.Response
	response, err = this.pubConn.Do(req)
	// when you get a redirection failure both response and err will be non-nil
	if response != nil {
		// reuse the connection
		defer response.Body.Close()
	}
	if err != nil {
		return
	}

	// TODO if 201, needn't read body
	var b []byte
	b, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}

	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusAccepted {
		return errors.New(string(b))
	}

	if this.cf.Debug {
		log.Printf("--> [%s]", response.Status)
		log.Printf("Partition:%s Offset:%s",
			response.Header.Get(gateway.HttpHeaderPartition),
			response.Header.Get(gateway.HttpHeaderOffset))
	}

	return nil
}
