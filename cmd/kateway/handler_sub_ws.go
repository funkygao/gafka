package main

import (
	"net/http"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/websocket"
	"github.com/julienschmidt/httprouter"
)

// /ws/msgs/:appid/:topic/:ver?group=xx
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
