package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/golib/set"
	"github.com/gorilla/mux"
)

type route struct {
	path   string
	method string
}

func (this *Gateway) registerRoute(router *mux.Router, path, method string,
	handler http.HandlerFunc) {
	this.routes = append(this.routes, route{
		path:   path,
		method: method,
	})

	router.HandleFunc(path, handler).Methods(method)
}

func (this *Gateway) buildRouting() {
	if this.pubServer != nil {
		this.registerRoute(this.pubServer.Router(),
			"/ver", "GET", this.versionHandler)
		this.registerRoute(this.pubServer.Router(),
			"/clusters", "GET", this.clustersHandler)
		this.registerRoute(this.pubServer.Router(),
			"/help", "GET", this.helpHandler)
		this.registerRoute(this.pubServer.Router(),
			"/stat", "GET", this.statHandler)
		this.registerRoute(this.pubServer.Router(),
			"/ping", "GET", this.pingHandler)

		this.registerRoute(this.pubServer.Router(),
			"/topics/{ver}/{topic}", "POST", this.pubHandler)
		this.registerRoute(this.pubServer.Router(),
			"/ws/topics/{ver}/{topic}", "POST", this.pubWsHandler)
	}

	if this.subServer != nil {
		this.registerRoute(this.subServer.Router(),
			"/ver", "GET", this.versionHandler)
		this.registerRoute(this.subServer.Router(),
			"/clusters", "GET", this.clustersHandler)
		this.registerRoute(this.subServer.Router(),
			"/help", "GET", this.helpHandler)
		this.registerRoute(this.subServer.Router(),
			"/stat", "GET", this.statHandler)
		this.registerRoute(this.subServer.Router(),
			"/ping", "GET", this.pingHandler)

		this.registerRoute(this.subServer.Router(),
			"/topics/{ver}/{topic}/raw", "GET", this.subRawHandler)
		this.registerRoute(this.subServer.Router(),
			"/topics/{ver}/{topic}/{group}", "GET", this.subHandler)
		this.registerRoute(this.subServer.Router(),
			"/ws/topics/{ver}/{topic}/{group}", "GET", this.subWsHandler)
	}

}

func (this *Gateway) helpHandler(w http.ResponseWriter, r *http.Request) {
	this.writeKatewayHeader(w)
	w.Header().Set("Content-Type", "text/plain")
	paths := set.NewSet()
	for _, route := range this.routes {
		if paths.Contains(route.path) {
			continue
		}

		paths.Add(route.path)
		w.Write([]byte(fmt.Sprintf("%4s %s\n", route.method, route.path)))
	}
}

func (this *Gateway) pingHandler(w http.ResponseWriter, r *http.Request) {
	this.writeKatewayHeader(w)
	w.Write([]byte(fmt.Sprintf("ver:%s, build:%s, uptime:%s",
		gafka.Version, gafka.BuildId, time.Since(this.startedAt))))
}

func (this *Gateway) versionHandler(w http.ResponseWriter, r *http.Request) {
	this.writeKatewayHeader(w)
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(fmt.Sprintf("%s-%s", gafka.Version, gafka.BuildId)))
}

func (this *Gateway) clustersHandler(w http.ResponseWriter, r *http.Request) {
	this.writeKatewayHeader(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	b, _ := json.Marshal(this.meta.Clusters())
	w.Write(b)
}

func (this *Gateway) statHandler(w http.ResponseWriter, r *http.Request) {
	this.writeKatewayHeader(w)
	w.Header().Set("Content-Type", "application/json")
	this.guard.Refresh()
	b, _ := json.Marshal(this.guard.cpuStat)
	w.Write(b)
}
