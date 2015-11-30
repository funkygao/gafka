package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"syscall"
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
	if options.pubHttpAddr != "" || options.pubHttpsAddr != "" {
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

	if options.subHttpAddr != "" || options.subHttpsAddr != "" {
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
			"/topics/{ver}/{topic}/{group}", "GET", this.subHandler)
		this.registerRoute(this.subServer.Router(),
			"/ws/topics/{ver}/{topic}/{group}", "GET", this.subWsHandler)
	}

}

func (this *Gateway) helpHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "kateway")
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

func (this *Gateway) pingHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "kateway")
	w.Write([]byte(fmt.Sprintf("ver:%s, build:%s, uptime:%s",
		gafka.Version, gafka.BuildId, time.Since(this.startedAt))))
}

func (this *Gateway) versionHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "kateway")
	w.Header().Set("Content-Type", "text/text")
	w.Write([]byte(fmt.Sprintf("%s-%s", gafka.Version, gafka.BuildId)))
}

func (this *Gateway) clustersHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "kateway")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	b, _ := json.Marshal(this.metaStore.Clusters())
	w.Write(b)
}

func (this *Gateway) statHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "kateway")
	rusage := &syscall.Rusage{}
	var interval time.Duration
	if this.lastStatExec.Nanosecond() == 0 {
		interval = time.Since(this.startedAt)
	} else {
		interval = time.Since(this.lastStatExec)
	}
	this.lastStatExec = time.Now()
	syscall.Getrusage(syscall.RUSAGE_SELF, rusage)
	syscall.Getrusage(syscall.RUSAGE_SELF, rusage)
	userTime := rusage.Utime.Sec*1000000000 + int64(rusage.Utime.Usec)
	sysTime := rusage.Stime.Sec*1000000000 + int64(rusage.Stime.Usec)
	userCpuUtil := float64(userTime-this.lastUserTime) * 100 / float64(interval)
	sysCpuUtil := float64(sysTime-this.lastSysTime) * 100 / float64(interval)
	w.Write([]byte(fmt.Sprintf("us:%3.2f%% sy:%3.2f%%", userCpuUtil, sysCpuUtil)))
}
