package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/mpool"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

type pubResponse struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// /topics/:topic/:ver?key=mykey&async=1
func (this *Gateway) pubHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	if this.breaker.Open() {
		this.writeBreakerOpen(w)
		return
	}

	var (
		topic string
		ver   string
		key   string
		appid string
		async bool
	)

	topic = params.ByName("topic")
	ver = params.ByName("ver")
	appid = r.Header.Get("Appid")
	key = r.URL.Query().Get("key") // if key given, batched msg must belong to same key
	async = r.URL.Query().Get("async") == "1"

	if !meta.Default.AuthPub(appid, r.Header.Get("Pubkey"), topic) {
		this.writeAuthFailure(w)
		return
	}

	// get the raw POST message
	lbr := io.LimitReader(r.Body, options.maxPubSize+1)
	var (
		rawMsg []byte
		err    error
	)
	if false {
		bufp := mpool.BytesBufferGet()
		defer mpool.BytesBufferPut(bufp) // TODO defer is heavy

		bufp.Reset()
		if _, err = io.Copy(bufp, lbr); err != nil {
			log.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		rawMsg = bufp.Bytes()
	} else {
		rawMsg, err = ioutil.ReadAll(lbr) // TODO optimize
		if err != nil {
			this.writeBadRequest(w, ErrTooBigPubMessage)
			return
		}
	}

	log.Debug("pub[%s] %s %+v %s", appid, r.RemoteAddr, params, string(rawMsg))

	t1 := time.Now()
	this.pubMetrics.PubConcurrent.Inc(1)

	this.pubMetrics.PubQps.Mark(1)
	this.pubMetrics.PubSize.Mark(int64(len(rawMsg)))
	rawTopic := meta.KafkaTopic(appid, topic, ver)
	pubMethod := store.DefaultPubStore.SyncPub
	if async {
		pubMethod = store.DefaultPubStore.AsyncPub
	}
	partition, offset, err := pubMethod(options.cluster, rawTopic, key, rawMsg) // FIXME
	if err != nil {
		if isBrokerError(err) {
			this.breaker.Fail()
		}

		this.pubMetrics.PubConcurrent.Dec(1)
		this.pubMetrics.PubFailure.Inc(1)
		log.Error("%s: %v", r.RemoteAddr, err)

		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	response := pubResponse{
		Partition: partition,
		Offset:    offset,
	}
	b, _ := json.Marshal(response)
	if _, err := w.Write(b); err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		this.pubMetrics.ClientError.Inc(1)
	}

	this.pubMetrics.PubSuccess.Inc(1)
	this.pubMetrics.PubConcurrent.Dec(1)
	this.pubMetrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6) // in ms
}
