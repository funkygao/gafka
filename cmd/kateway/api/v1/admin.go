package api

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/funkygao/gafka/cmd/kateway/gateway"
)

func (this *Client) AddTopic(cluster, appid, topic, ver string) (err error) {
	var req *http.Request
	var u url.URL
	u.Scheme = this.cf.Admin.Scheme
	u.Host = this.cf.Admin.Endpoint
	u.Path = fmt.Sprintf("/v1/topics/%s/%s/%s/%s", cluster, appid, topic, ver)
	q := u.Query()
	q.Set("replicas", "1")
	u.RawQuery = q.Encode()

	req, err = http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return
	}

	req.Header.Set(gateway.HttpHeaderAppid, this.cf.AppId)
	req.Header.Set(gateway.HttpHeaderPubkey, this.cf.Secret)

	var response *http.Response
	response, err = this.adminConn.Do(req)
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

	if response.StatusCode != http.StatusOK {
		return errors.New(string(b))
	}

	return nil
}
