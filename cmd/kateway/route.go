package main

import (
	"encoding/json"
	"fmt"
	"net/http"

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
	if options.pubHttpAddr != "" || options.pubHttpsAddr != "" {
		this.registerRoute(this.pubServer.Router(),
			"/ver", "GET", this.showVersion)
		this.registerRoute(this.pubServer.Router(),
			"/clusters", "GET", this.showClusters)
		this.registerRoute(this.pubServer.Router(),
			"/help", "GET", this.showHelp)

		this.registerRoute(this.pubServer.Router(),
			"/{ver}/topics/{topic}", "POST", this.pubHandler)
		this.registerRoute(this.pubServer.Router(),
			"/ws/{ver}/topics/{topic}", "POST", this.pubWsHandler)
	}

	if options.subHttpAddr != "" || options.subHttpsAddr != "" {
		this.registerRoute(this.subServer.Router(),
			"/ver", "GET", this.showVersion)
		this.registerRoute(this.subServer.Router(),
			"/clusters", "GET", this.showClusters)
		this.registerRoute(this.subServer.Router(),
			"/help", "GET", this.showHelp)

		this.registerRoute(this.subServer.Router(),
			"/{ver}/topics/{topic}/{group}", "GET", this.subHandler)
		this.registerRoute(this.subServer.Router(),
			"/ws/{ver}/topics/{topic}/{group}", "GET", this.subWsHandler)
	}

}

func (this *Gateway) showHelp(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/text")
	paths := set.NewSet()
	for _, route := range this.routes {
		if paths.Contains(route.path) {
			continue
		}

		paths.Add(route.path)
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
