package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/funkygao/gafka"
)

func (this *Gateway) buildRouting() {
	this.router.HandleFunc("/ver", this.showVersion)
	this.router.HandleFunc("/clusters", this.showClusters)
	this.router.HandleFunc("/help", this.showHelp)

	switch this.mode {
	case "pub":
		this.router.HandleFunc("/{ver}/topics/{topic}", this.pubHandler).Methods("POST")

	case "sub":
		this.router.HandleFunc("/{ver}/topics/{topic}/{group}",
			this.subHandler).Methods("GET")

	case "pubsub":
		this.router.HandleFunc("/{ver}/topics/{topic}", this.pubHandler).Methods("POST")
		this.router.HandleFunc("/{ver}/topics/{topic}/{group}",
			this.subHandler).Methods("GET")
	}
}

func (this *Gateway) showHelp(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/text")
	txt := strings.TrimSpace(fmt.Sprintf(`
POST /{ver}/topics/{topic}
GET  /{ver}/topics/{topic}/{group}
GET  /clusters
GET  /ver
GET  /help
		`))
	w.Write([]byte(txt))
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
