package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/mpool"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
)

type ackOffset struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`

	cluster string
	topic   string
	group   string
}

type ackOffsets []ackOffset

//go:generate goannotation $GOFILE
// @rest PUT /v1/offsets/:appid/:topic/:ver/:group with json body
func (this *subServer) ackHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		topic    string
		ver      string
		myAppid  string
		hisAppid string
		group    string
		err      error
	)

	group = params.ByName(UrlParamGroup)
	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		writeBadRequest(w, "invalid appid")
		return
	}

	msgLen := int(r.ContentLength)
	switch {
	case int64(msgLen) > Options.MaxPubSize:
		writeBadRequest(w, ErrTooBigMessage.Error())
		return

	case msgLen < Options.MinPubSize:
		writeBadRequest(w, ErrTooSmallMessage.Error())
		return
	}

	var msg *mpool.Message
	msg = mpool.NewMessage(msgLen)
	msg.Body = msg.Body[0:msgLen]
	lbr := io.LimitReader(r.Body, Options.MaxPubSize+1)
	if _, err = io.ReadAtLeast(lbr, msg.Body, msgLen); err != nil {
		msg.Free()

		writeBadRequest(w, ErrTooBigMessage.Error())
		return
	}

	var acks ackOffsets
	if err = json.Unmarshal(msg.Body, &acks); err != nil {
		msg.Free()

		writeBadRequest(w, "invalid ack json body")
		return
	}

	msg.Free()

	realIp := getHttpRemoteIp(r)
	realGroup := myAppid + "." + group
	rawTopic := manager.Default.KafkaTopic(hisAppid, topic, ver)
	for i := 0; i < len(acks); i++ {
		acks[i].cluster = cluster
		acks[i].topic = rawTopic
		acks[i].group = realGroup
	}

	log.Debug("ack[%s/%s] %s(%s) {%s.%s.%s UA:%s} %+v",
		myAppid, group, r.RemoteAddr, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"), acks)

	if atomic.AddInt32(&this.ackShutdown, 1) == 0 {
		// kateway is shutting down, ackCh is already closed
		log.Warn("ack[%s/%s] %s(%s) {%s.%s.%s UA:%s} server is shutting down %+v ",
			myAppid, group, r.RemoteAddr, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"), acks)

		writeServerError(w, "server is shutting down")
		return
	}

	this.ackCh <- acks
	atomic.AddInt32(&this.ackShutdown, -1)

	w.Write(ResponseOk)
}

//go:generate goannotation $GOFILE
// @rest PUT /v1/raw/offsets/:cluster/:topic/:group with json body
func (this *subServer) ackRawHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		topic   string
		cluster string
		myAppid string
		group   string
		err     error
	)

	group = params.ByName(UrlParamGroup)
	cluster = params.ByName("cluster")
	topic = params.ByName(UrlParamTopic)
	myAppid = r.Header.Get(HttpHeaderAppid)

	msgLen := int(r.ContentLength)
	switch {
	case int64(msgLen) > Options.MaxPubSize:
		writeBadRequest(w, ErrTooBigMessage.Error())
		return

	case msgLen < Options.MinPubSize:
		writeBadRequest(w, ErrTooSmallMessage.Error())
		return
	}

	var msg *mpool.Message
	msg = mpool.NewMessage(msgLen)
	msg.Body = msg.Body[0:msgLen]
	lbr := io.LimitReader(r.Body, Options.MaxPubSize+1)
	if _, err = io.ReadAtLeast(lbr, msg.Body, msgLen); err != nil {
		msg.Free()

		writeBadRequest(w, ErrTooBigMessage.Error())
		return
	}

	var acks ackOffsets
	if err = json.Unmarshal(msg.Body, &acks); err != nil {
		msg.Free()

		writeBadRequest(w, "invalid ack json body")
		return
	}

	msg.Free()

	realIp := getHttpRemoteIp(r)
	realGroup := myAppid + "." + group
	for i := 0; i < len(acks); i++ {
		acks[i].cluster = cluster
		acks[i].topic = topic
		acks[i].group = realGroup
	}

	log.Debug("ack raw[%s/%s] %s(%s) {%s/%s UA:%s} %+v",
		myAppid, group, r.RemoteAddr, realIp, cluster, topic, r.Header.Get("User-Agent"), acks)

	if atomic.AddInt32(&this.ackShutdown, 1) == 0 {
		writeServerError(w, "server is shutting down")
		return
	}

	this.ackCh <- acks
	atomic.AddInt32(&this.ackShutdown, -1)

	w.Write(ResponseOk)
}
