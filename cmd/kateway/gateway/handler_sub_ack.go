package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/mpool"
	//log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

type ackOffset struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

type ackOffsets []ackOffset

// PUT /v1/offsets/:appid/:topic/:ver/:group with json body
func (this *subServer) ackHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
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
		this.gw.writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		this.gw.writeBadRequest(w, "invalid appid")
		return
	}

	msgLen := int(r.ContentLength)
	switch {
	case int64(msgLen) > Options.MaxPubSize:
		this.gw.writeBadRequest(w, ErrTooBigMessage.Error())
		return

	case msgLen < Options.MinPubSize:
		this.gw.writeBadRequest(w, ErrTooSmallMessage.Error())
		return
	}

	var msg *mpool.Message
	msg = mpool.NewMessage(msgLen)
	msg.Body = msg.Body[0:msgLen]
	lbr := io.LimitReader(r.Body, Options.MaxPubSize+1)
	if _, err := io.ReadAtLeast(lbr, msg.Body, msgLen); err != nil {
		msg.Free()

		this.gw.writeBadRequest(w, ErrTooBigMessage.Error())
		return
	}

	var acks ackOffsets
	if err = json.Unmarshal(msg.Body, &acks); err != nil {
		msg.Free()

		this.gw.writeBadRequest(w, "invalid json body")
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	realGroup := myAppid + "." + group
	rawTopic := manager.KafkaTopic(hisAppid, topic, ver)
	for _, ack := range acks {
		err = zkcluster.ResetConsumerGroupOffset(rawTopic, realGroup,
			strconv.Itoa(ack.Partition), ack.Offset)
		if err != nil {
			msg.Free()

			this.gw.writeServerError(w, err.Error())
			return
		}
	}

	msg.Free()
	w.Write(ResponseOk)
}
