package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/funkygao/gafka"
)

type route struct {
	path   string
	method string
}

func (this *Gateway) registerRoute(path, method string,
	handler func(http.ResponseWriter, *http.Request)) {
	this.routes = append(this.routes, route{
		path:   path,
		method: method,
	})

	this.router.HandleFunc(path, handler).Methods(method)
}

func (this *Gateway) buildRouting() {
	this.registerRoute("/ver", "GET", this.showVersion)
	this.registerRoute("/clusters", "GET", this.showClusters)
	this.registerRoute("/help", "GET", this.showHelp)

	switch this.mode {
	case "pub":
		this.registerRoute("/{ver}/topics/{topic}", "POST", this.pubHandler)
		this.registerRoute("/ws/{ver}/topics/{topic}", "POST", this.pubWsHandler)

	case "sub":
		this.registerRoute("/{ver}/topics/{topic}/{group}", "GET", this.subHandler)
		this.registerRoute("/ws/{ver}/topics/{topic}/{group}", "GET", this.subWsHandler)

	case "pubsub":
		this.registerRoute("/{ver}/topics/{topic}", "POST", this.pubHandler)
		this.registerRoute("/ws/{ver}/topics/{topic}", "POST", this.pubWsHandler)

		this.registerRoute("/{ver}/topics/{topic}/{group}", "GET", this.subHandler)
		this.registerRoute("/ws/{ver}/topics/{topic}/{group}", "GET", this.subWsHandler)
	}
}

func (this *Gateway) showHelp(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/text")
	for _, route := range this.routes {
		w.Write([]byte(fmt.Sprintf("%4s %s\n", route.method, route.path)))
	}
}

func (this *Gateway) showVersion(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/text")
	w.Write([]byte(fmt.Sprintf("%s-%s", gafka.Version, gafka.BuildId)))
}

func (this *Gateway) showClusters(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	b, _ := json.Marshal(this.metaStore.Clusters())
	w.Write(b)
}
