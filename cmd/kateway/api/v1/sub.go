package api

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/funkygao/gafka/cmd/kateway/gateway"
	"github.com/funkygao/gafka/sla"
	"github.com/funkygao/gorequest"
)

type SubOption struct {
	AppId      string
	Topic, Ver string
	Group      string
	Reset      string // newest | oldest
	Shadow     string
}

type SubHandler func(statusCode int, msg []byte) error

func (this *Client) Sub(opt SubOption, h SubHandler) error {
	var u url.URL
	u.Scheme = this.cf.Sub.Scheme
	u.Host = this.cf.Sub.Endpoint
	u.Path = fmt.Sprintf("/v1/msgs/%s/%s/%s", opt.AppId, opt.Topic, opt.Ver)
	q := u.Query()
	q.Set("group", opt.Group)
	if opt.Shadow != "" && sla.ValidateShadowName(opt.Shadow) {
		q.Set("use", opt.Shadow)
	}
	if opt.Reset != "" {
		q.Set("reset", opt.Reset)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}

	req.Header.Set(gateway.HttpHeaderAppid, this.cf.AppId)
	req.Header.Set(gateway.HttpHeaderSubkey, this.cf.Secret)
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
	Bury      string
	Offset    string
	Partition string
	Tag       string
}

func (this *SubXResult) Reset() {
	this.Bury = ""
}

// SubX is advanced Sub with features of delayed ack and shadow bury.
func (this *Client) SubX(opt SubOption, h SubXHandler) error {
	var u url.URL
	u.Scheme = this.cf.Sub.Scheme
	u.Host = this.cf.Sub.Endpoint
	u.Path = fmt.Sprintf("/v1/msgs/%s/%s/%s", opt.AppId, opt.Topic, opt.Ver)
	q := u.Query()
	q.Set("group", opt.Group)
	q.Set("ack", "1")
	if opt.Shadow != "" && sla.ValidateShadowName(opt.Shadow) {
		q.Set("q", opt.Shadow)
	}
	if opt.Reset != "" {
		q.Set("reset", opt.Reset)
	}
	u.RawQuery = q.Encode()

	req := gorequest.New()
	req.Get(u.String()).
		Set(gateway.HttpHeaderAppid, this.cf.AppId).
		Set(gateway.HttpHeaderSubkey, this.cf.Secret).
		Set("User-Agent", UserAgent).
		Set(gateway.HttpHeaderPartition, "-1").
		Set(gateway.HttpHeaderOffset, "-1")
	r := &SubXResult{}
	for {
		response, b, errs := req.EndBytes()
		if len(errs) > 0 {
			return errs[0]
		}

		// reset the request header
		req.Set(gateway.HttpHeaderPartition, "")
		req.Set(gateway.HttpHeaderOffset, "")
		req.Set(gateway.HttpHeaderMsgBury, "")

		if this.cf.Debug {
			log.Printf("--> [%s]", response.Status)
			log.Printf("Partition:%s Offset:%s",
				response.Header.Get(gateway.HttpHeaderPartition),
				response.Header.Get(gateway.HttpHeaderOffset))
		}

		r.Reset()
		r.Partition = response.Header.Get(gateway.HttpHeaderPartition)
		r.Offset = response.Header.Get(gateway.HttpHeaderOffset)
		r.Tag = response.Header.Get(gateway.HttpHeaderMsgTag)
		if err := h(response.StatusCode, b, r); err != nil {
			return err
		}

		req.Set(gateway.HttpHeaderPartition, r.Partition)
		req.Set(gateway.HttpHeaderOffset, r.Offset)

		if r.Bury != "" {
			if r.Bury != ShadowRetry && r.Bury != ShadowDead {
				return ErrInvalidBury
			}

			req.Set("X-Bury", r.Bury)
		}
	}

}
