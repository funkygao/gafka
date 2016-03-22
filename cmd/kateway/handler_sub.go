package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/inflights"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/sla"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/websocket"
	"github.com/julienschmidt/httprouter"
)

// /topics/:appid/:topic/:ver?group=xx&&reset=<newest|oldest>&ack=1&use=<dead|retry>
func (this *Gateway) subHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic      string
		origTopic  string
		ver        string
		myAppid    string
		hisAppid   string
		reset      string
		group      string
		guardName  string
		partition  string
		partitionN int = -1
		offset     string
		offsetN    int64 = -1
		ack        string
		delayedAck bool
		err        error
	)

	if options.EnableClientStats {
		this.clientStates.RegisterSubClient(r)
	}

	query := r.URL.Query()
	group = query.Get("group")
	reset = query.Get("reset")
	if !validateGroupName(group) {
		this.writeBadRequest(w, "illegal group")
		return
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	origTopic = topic
	hisAppid = params.ByName(UrlParamAppid)
	guardName = query.Get("use")
	ack = query.Get("ack")
	myAppid = r.Header.Get(HttpHeaderAppid)
	if r.Header.Get("Connection") == "close" {
		// sub should use keep-alive
		log.Warn("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} not keep-alive",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)
	}

	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic); err != nil {
		log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		this.writeAuthFailure(w, err)
		return
	}

	delayedAck = ack == "1"
	if delayedAck {
		partition = r.Header.Get(HttpHeaderPartition)
		offset = r.Header.Get(HttpHeaderOffset)
		if partition != "" && offset != "" {
			// convert partition and offset to int

			offsetN, err = strconv.ParseInt(offset, 10, 64)
			if err != nil || offsetN < 0 {
				log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} offset:%s",
					myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, offset)

				this.writeBadRequest(w, "ack with bad offset")
				return
			}
			partitionN, err = strconv.Atoi(partition)
			if err != nil || partitionN < 0 {
				log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} partition:%s",
					myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, partition)

				this.writeBadRequest(w, "ack with bad partition")
				return
			}
		}
	}

	log.Debug("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s ack:%s, partition:%s, offset:%s}",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
		group, ack, partition, offset)

	if guardName != "" {
		if !sla.ValidateGuardName(guardName) {
			log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s use:%s} invalid guard name",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, guardName)

			this.writeBadRequest(w, "invalid guard name")
			return
		}

		if !manager.Default.IsGuardedTopic(hisAppid, topic, ver, group) {
			log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s use:%s} not a guarded topic",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, guardName)

			this.writeBadRequest(w, "register guard before sub guarded topic")
			return
		}

		topic = topic + "." + guardName
	}
	rawTopic := meta.KafkaTopic(hisAppid, topic, ver)

	// pick a consumer from the consumer group
	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} cluster not found",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)

		this.writeBadRequest(w, "invalid appid")
		return
	}

	fetcher, err := store.DefaultSubStore.Fetch(cluster, rawTopic,
		myAppid+"."+group, r.RemoteAddr, reset)
	if err != nil {
		log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		this.writeBadRequest(w, err.Error())
		return
	}

	if delayedAck && partitionN >= 0 && offsetN >= 0 {
		if shadow := r.Header.Get(HttpHeaderMsgMove); shadow != "" {
			if shadow != sla.SlaKeyDeadLetterTopic && shadow != sla.SlaKeyRetryTopic {
				log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} illegal move: %s",
					myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err, shadow)

				this.writeBadRequest(w, "illegal move")
				return
			}

			msg, err := inflights.Default.LandX(cluster, rawTopic, group, partition, offsetN)
			if err != nil {
				log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
					myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

				// will deadloop? FIXME
				this.writeBadRequest(w, err.Error())
				return
			}

			shadowTopic := meta.ShadowTopic(shadow, myAppid, hisAppid, origTopic, ver, group)
			_, _, err = store.DefaultPubStore.SyncPub(cluster, shadowTopic, nil, msg)
			if err != nil {
				log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
					myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

				this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// skip this message in the master topic
			if err = fetcher.CommitUpto(&sarama.ConsumerMessage{
				Topic:     rawTopic,
				Partition: int32(partitionN),
				Offset:    offsetN,
			}); err != nil {
				log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
					myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)
			}

			w.Write(ResponseOk)
			return
		}

		// what if shutdown kateway now?
		// the commit will be ok, and when pumpMessages, the conn will get http.StatusNoContent
		if err = fetcher.CommitUpto(&sarama.ConsumerMessage{
			Topic:     rawTopic,
			Partition: int32(partitionN),
			Offset:    offsetN,
		}); err != nil {
			log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)
		}

		log.Debug("land {G:%s, T:%s, P:%s, O:%s}", group, rawTopic, partition, offset)
		if err = inflights.Default.Land(cluster, rawTopic, group, partition, offsetN); err != nil {
			log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)
		}
	}

	err = this.pumpMessages(w, fetcher, myAppid, hisAppid, cluster, topic, ver, group, delayedAck)
	if err != nil {
		// e,g. broken pipe, io timeout, client gone
		log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		if err = fetcher.Close(); err != nil {
			log.Error("sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)
		}

		this.writeErrorResponse(w, "store error encountered", http.StatusInternalServerError)
	}

	return
}

func (this *Gateway) pumpMessages(w http.ResponseWriter, fetcher store.Fetcher,
	myAppid, hisAppid, cluster, topic, ver, group string, delayedAck bool) (err error) {
	clientGoneCh := w.(http.CloseNotifier).CloseNotify()

	select {
	case <-clientGoneCh:
		// FIXME access log will not be able to record this behavior
		err = ErrClientGone

	case <-this.shutdownCh:
		w.WriteHeader(http.StatusNoContent)
		w.Write([]byte{})

	case <-this.timer.After(options.SubTimeout):
		w.WriteHeader(http.StatusNoContent)
		w.Write([]byte{}) // without this, client cant get response

	case msg := <-fetcher.Messages():
		partition := strconv.FormatInt(int64(msg.Partition), 10)

		if delayedAck {
			log.Debug("take off {G:%s, T:%s, P:%d, O:%d}", group, msg.Topic, msg.Partition, msg.Offset)
			if err = inflights.Default.TakeOff(cluster, hisAppid+"."+topic+"."+ver, group,
				partition, msg.Offset, msg.Value); err != nil {
				// keep consuming the same message, offset never move ahead
				return
			}
		}

		w.Header().Set(HttpHeaderMsgKey, string(msg.Key))
		w.Header().Set(HttpHeaderPartition, partition)
		w.Header().Set(HttpHeaderOffset, strconv.FormatInt(msg.Offset, 10))

		// TODO when remote close silently, the write still ok
		// which will lead to msg lost for sub
		if _, err = w.Write(msg.Value); err != nil {
			return
		}

		if !delayedAck {
			log.Debug("commit offset {G:%s, T:%s, P:%d, O:%d}", group, msg.Topic, msg.Partition, msg.Offset)
			if err = fetcher.CommitUpto(msg); err != nil {
				log.Error("commit offset {T:%s, P:%d, O:%d}: %v", msg.Topic, msg.Partition, msg.Offset, err)
			}
		}

		this.subMetrics.ConsumeOk(myAppid, topic, ver)
		this.subMetrics.ConsumedOk(hisAppid, topic, ver)

	case err = <-fetcher.Errors():
		// e,g. consume a non-existent topic
		// e,g. conn with broker is broken
	}

	return

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
	group = query.Get("group")
	resetOffset = query.Get("reset")
	limit, err := getHttpQueryInt(&query, "limit", 1)
	if err != nil {
		this.writeWsError(ws, err.Error())
		return
	}
	if !validateGroupName(group) {
		log.Warn("consumer %s{topic:%s, ver:%s, group:%s, limit:%d} invalid group",
			r.RemoteAddr, topic, ver, group, limit)
		return
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)
	if err := manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic); err != nil {
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

	return
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
