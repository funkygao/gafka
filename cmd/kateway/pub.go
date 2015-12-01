package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

type pubResponse struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// /{ver}/topics/{topic}?key=xxx
func (this *Gateway) pubHandler(w http.ResponseWriter, r *http.Request) {
	if this.breaker.Open() {
		writeBreakerOpen(w)
		return
	}

	var (
		topic string
		key   string
	)

	params := mux.Vars(r)
	//ver := params["ver"] // TODO
	topic = params["topic"]
	key = r.URL.Query().Get("key") // if key given, batched msg must belong to same key

	if !this.authPub(r.Header.Get("Pubkey"), topic) {
		writeAuthFailure(w)
		return
	}

	// get the raw POST message
	pr := io.LimitReader(r.Body, options.maxPubSize+1)
	rawMsg, err := ioutil.ReadAll(pr) // TODO optimize
	if err != nil {
		writeBadRequest(w)
		return
	}

	t1 := time.Now()
	this.pubMetrics.PubConcurrent.Inc(1)

	// TODO some topics use async put
	partition, offset, err := this.pubStore.SyncPub(options.cluster, topic, key, rawMsg) // FIXME
	if err != nil {
		if isBrokerError(err) {
			this.breaker.Fail()
		}

		this.pubMetrics.PubConcurrent.Dec(1)
		this.pubMetrics.PubFailure.Inc(1)
		log.Error("%s: %v", r.RemoteAddr, err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

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
