package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/websocket"
	"github.com/julienschmidt/httprouter"
)

// /topics/:appid/:topic/:ver?group=xx&limit=1&reset=newest
func (this *Gateway) subHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic    string
		ver      string
		myAppid  string
		hisAppid string
		reset    string
		group    string
		err      error
	)

	if options.EnableClientStats {
		this.clientStates.registerSubClient(r)
	}

	query := r.URL.Query()
	group = query.Get(UrlQueryGroup)
	reset = query.Get(UrlQueryReset)
	limit, err := getHttpQueryInt(&query, "limit", 1)
	if err != nil {
		this.writeBadRequest(w, err)
		return
	}
	if group == "" || strings.Contains(group, ".") { // TODO more strict rules
		log.Warn("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} invalid group name",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)

		http.Error(w, "invalid group name", http.StatusBadRequest)
		return
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)
	if r.Header.Get(HttpHeaderConnection) == "close" {
		// sub should use keep-alive
		log.Warn("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} not keep-alive",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)
	}

	if err := manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey), topic); err != nil {
		log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		this.writeAuthFailure(w, err)
		return
	}

	if options.Debug || true {
		log.Debug("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s}",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)
	}

	rawTopic := meta.KafkaTopic(hisAppid, topic, ver)
	// pick a consumer from the consumer group
	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} cluster not found",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)

		http.Error(w, "invalid subd appid", http.StatusBadRequest)
		return
	}

	fetcher, err := store.DefaultSubStore.Fetch(cluster, rawTopic,
		myAppid+"."+group, r.RemoteAddr, reset)
	if err != nil {
		log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		this.writeBadRequest(w, err)
		return
	}

	err = this.fetchMessages(w, fetcher, limit, myAppid, hisAppid, topic, ver)
	if err != nil {
		// e,g. broken pipe, io timeout, client gone
		log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		go fetcher.Close() // wait cf.ProcessingTimeout FIXME go?
	}

}

func (this *Gateway) fetchMessages(w http.ResponseWriter, fetcher store.Fetcher,
	limit int, myAppid, hisAppid, topic, ver string) error {
	clientGoneCh := w.(http.CloseNotifier).CloseNotify()

	var (
		chunkedBeforeTimeout = false
		chunkedEver          = false
		n                    = 0
		err                  error
	)
	for {
		select {
		case <-clientGoneCh:
			return ErrClientGone

		case <-this.shutdownCh:
			if !chunkedEver {
				w.WriteHeader(http.StatusNoContent)
				w.Write([]byte{})
			}
			return nil

		case msg := <-fetcher.Messages():
			// TODO when remote close silently, the write still ok
			// which will lead to msg losing for sub
			if _, err := w.Write(msg.Value); err != nil {
				// TODO if cf.ChannelBufferSize > 0, client may lose message
				// got message in chan, client not recv it but offset commited.
				return err
			}

			// client really got this msg, safe to commit
			// TODO test case: client got chunk 2, then killed. should server commit offset?
			log.Debug("commit offset: {T:%s, P:%d, O:%d}", msg.Topic, msg.Partition, msg.Offset)
			if err = fetcher.CommitUpto(msg); err != nil {
				log.Error("commit offset {T:%s, P:%d, O:%d}: %v", msg.Topic, msg.Partition, msg.Offset, err)
			}

			this.subMetrics.ConsumeOk(myAppid, topic, ver)
			this.subMetrics.ConsumedOk(hisAppid, topic, ver)

			n++
			if n >= limit {
				return nil
			}

			// http chunked: len in hex
			// curl CURLOPT_HTTP_TRANSFER_DECODING will auto unchunk
			w.(http.Flusher).Flush()

			chunkedBeforeTimeout = true
			chunkedEver = true

		case <-this.timer.After(options.SubTimeout):
			if chunkedBeforeTimeout {
				log.Debug("await message timeout, chunked to next round")

				chunkedBeforeTimeout = false
				continue
			}

			if chunkedEver {
				// response already sent in chunk
				log.Debug("await message timeout, chunk finished")
				return nil
			}

			// never chunked, so send empty data
			log.Debug("await message timeout, writing empty data")
			w.WriteHeader(http.StatusNoContent)
			// TODO write might fail, remote client might have died
			w.Write([]byte{}) // without this, client cant get response
			return nil

		case err = <-fetcher.Errors():
			// e,g. consume a non-existent topic
			return err
		}
	}

}

// /raw/topics/:appid/:topic/:ver
// tells client how to sub in raw mode: how to connect kafka
func (this *Gateway) subRawHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic    string
		ver      string
		hisAppid string
		myAppid  string
	)

	if options.EnableClientStats {
		this.clientStates.registerSubClient(r)
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	if err := manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey), topic); err != nil {
		log.Error("consumer[%s] %s {topic:%s, ver:%s, hisapp:%s}: %s",
			myAppid, r.RemoteAddr, topic, ver, hisAppid, err)

		this.writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("cluster not found for subd app: %s", hisAppid)

		http.Error(w, "invalid subd appid", http.StatusBadRequest)
		return
	}

	this.writeKatewayHeader(w)
	var out = map[string]string{
		"store": "kafka",
		"zk":    meta.Default.ZkCluster(cluster).ZkConnectAddr(),
		"topic": meta.KafkaTopic(hisAppid, topic, ver),
	}
	b, _ := json.Marshal(out)
	w.Header().Set(ContentTypeText, ContentTypeJson)
	w.Write(b)
}

// /ws/topics/:appid/:topic/:ver?group=xx
func (this *Gateway) subWsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		return
	}

	defer func() {
		ws.Close()

		this.svrMetrics.ConcurrentSubWs.Dec(1)
		this.subServer.idleConnsWg.Done()
	}()

	var (
		topic       string
		ver         string
		myAppid     string
		hisAppid    string
		resetOffset string
		group       string
	)

	query := r.URL.Query()
	group = query.Get(UrlQueryGroup)
	resetOffset = query.Get(UrlQueryReset)
	limit, err := getHttpQueryInt(&query, "limit", 1)
	if err != nil {
		this.writeWsError(ws, err.Error())
		return
	}
	if group == "" || strings.Contains(group, ".") { // TODO more strict rules
		log.Warn("consumer %s{topic:%s, ver:%s, group:%s, limit:%d} invalid group",
			r.RemoteAddr, topic, ver, group, limit)

		return
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)
	if err := manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey), topic); err != nil {
		log.Error("consumer[%s] %s {hisapp:%s, topic:%s, ver:%s, group:%s, limit:%d}: %s",
			myAppid, r.RemoteAddr, hisAppid, topic, ver, group, limit, err)

		this.writeWsError(ws, "auth fail")
		return
	}

	log.Debug("sub[%s] %s: %+v", myAppid, r.RemoteAddr, params)

	rawTopic := meta.KafkaTopic(hisAppid, topic, ver)
	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("cluster not found for subd app: %s", hisAppid)

		this.writeWsError(ws, "invalid subd appid")
		return
	}

	fetcher, err := store.DefaultSubStore.Fetch(cluster, rawTopic,
		myAppid+"."+group, r.RemoteAddr, resetOffset)
	if err != nil {
		log.Error("sub[%s] %s: %+v %v", myAppid, r.RemoteAddr, params, err)

		this.writeWsError(ws, err.Error())
		return
	}

	// kateway             sub client
	//   |                    |
	//   | ping               |
	//   |------------------->|
	//   |                    |
	//   |               pong |
	//   |<-------------------|
	//   |                    |
	//

	clientGone := make(chan struct{})
	go this.wsWritePump(clientGone, ws, fetcher)
	this.wsReadPump(clientGone, ws)
}

func (this *Gateway) wsReadPump(clientGone chan struct{}, ws *websocket.Conn) {
	ws.SetReadLimit(this.subServer.wsReadLimit)
	ws.SetReadDeadline(time.Now().Add(this.subServer.wsPongWait))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(this.subServer.wsPongWait))
		return nil
	})

	// if kateway shutdown while there are open ws conns, the shutdown will
	// wait 1m: this.subServer.wsPongWait
	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
				log.Warn("%s: %v", ws.RemoteAddr(), err)
			} else {
				log.Debug("%s: %v", ws.RemoteAddr(), err)
			}

			close(clientGone)
			break
		}

		log.Debug("ws[%s] read: %s", ws.RemoteAddr(), string(message))
	}
}

func (this *Gateway) wsWritePump(clientGone chan struct{}, ws *websocket.Conn, fetcher store.Fetcher) {
	defer fetcher.Close()

	var err error
	for {
		select {
		case msg := <-fetcher.Messages():
			ws.SetWriteDeadline(time.Now().Add(time.Second * 10))
			// FIXME because of buffer, client recv 10, but kateway written 100, then
			// client quit...
			if err = ws.WriteMessage(websocket.BinaryMessage, msg.Value); err != nil {
				log.Error("%s: %v", ws.RemoteAddr(), err)
				return
			}

			if err := fetcher.CommitUpto(msg); err != nil {
				log.Error(err) // TODO add more ctx
			}

		case err = <-fetcher.Errors():
			// TODO
			log.Error(err)

		case <-this.timer.After(this.subServer.wsPongWait / 3):
			ws.SetWriteDeadline(time.Now().Add(time.Second * 10))
			if err = ws.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				log.Error("%s: %v", ws.RemoteAddr(), err)
				return
			}

		case <-this.shutdownCh:
			return

		case <-clientGone:
			return
		}

	}

}

func (this *Gateway) writeWsError(ws *websocket.Conn, err string) {
	ws.WriteMessage(websocket.CloseMessage, []byte(err))
}
