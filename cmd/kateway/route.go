package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/julienschmidt/httprouter"
)

func (this *Gateway) buildRouting() {
	if this.pubServer != nil {
		this.pubServer.Router().GET("/ver", this.versionHandler)
		this.pubServer.Router().GET("/clusters", this.clustersHandler)
		this.pubServer.Router().GET("/help", this.helpHandler)
		this.pubServer.Router().GET("/stat", this.statHandler)
		this.pubServer.Router().GET("/ping", this.pingHandler)

		this.pubServer.Router().POST("/topics/:topic/:ver", this.pubHandler)
		this.pubServer.Router().POST("/ws/topics/:topic/:ver", this.pubWsHandler)
	}

	if this.subServer != nil {
		this.subServer.Router().GET("/ver", this.versionHandler)
		this.subServer.Router().GET("/clusters", this.clustersHandler)
		this.subServer.Router().GET("/help", this.helpHandler)
		this.subServer.Router().GET("/stat", this.statHandler)
		this.subServer.Router().GET("/ping", this.pingHandler)

		this.subServer.Router().GET("/raw/topics/:appid/:topic/:ver", this.subRawHandler)
		this.subServer.Router().GET("/topics/:appid/:topic/:ver/:group", this.subHandler)
		this.subServer.Router().GET("/ws/topics/:appid/:topic/:ver/:group", this.subWsHandler)
	}

}

func (this *Gateway) helpHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(strings.TrimSpace(fmt.Sprintf(`
 GET /ver
 GET /help
 GET /stat
 GET /ping
 GET /clusters

POST /topics/:topic/:ver?key=mykey&async=1
 GET /raw/topics/:appid/:topic/:ver
 GET /topics/:appid/:topic/:ver/:group?limit=1&reset=newest
`))))

}

func (this *Gateway) pingHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)
	w.Write([]byte(fmt.Sprintf("ver:%s, build:%s, uptime:%s",
		gafka.Version, gafka.BuildId, time.Since(this.startedAt))))
}

func (this *Gateway) versionHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(fmt.Sprintf("%s-%s", gafka.Version, gafka.BuildId)))
}

func (this *Gateway) clustersHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	b, _ := json.Marshal(meta.Default.Clusters())
	w.Write(b)
}

func (this *Gateway) statHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)
	w.Header().Set("Content-Type", "application/json")
	this.guard.Refresh()
	b, _ := json.Marshal(this.guard.cpuStat)
	w.Write(b)
}
