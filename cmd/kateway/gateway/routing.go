package gateway

import (
	"net/http"
	"net/http/pprof"

	"github.com/NYTimes/gziphandler"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
)

func (this *Gateway) buildRouting() {
	m := this.middleware

	if this.manServer != nil {
		this.manServer.Router().GET("/alive", m(this.checkAliveHandler))
		this.manServer.Router().NotFound = http.HandlerFunc(this.manServer.notFoundHandler)

		// api for 'gk kateway'
		this.manServer.Router().GET("/v1/clusters", m(this.manServer.clustersHandler))
		this.manServer.Router().GET("/v1/status", m(this.manServer.statusHandler))
		this.manServer.Router().PUT("/v1/options/:option/:value", m(this.manServer.setOptionHandler))

		// DEPRECATED
		this.manServer.Router().DELETE("/v1/counter/:name", m(this.manServer.resetCounterHandler))

		// api for pubsub manager
		this.manServer.Router().GET("/v1/partitions/:appid/:topic/:ver",
			m(this.manServer.partitionsHandler))
		this.manServer.Router().POST("/v1/topics/:appid/:topic/:ver",
			m(this.manServer.createTopicHandler))
		this.manServer.Router().PUT("/v1/topics/:appid/:topic/:ver",
			m(this.manServer.alterTopicHandler))
		this.manServer.Router().POST("/v1/jobs/:appid/:topic/:ver",
			this.manServer.createJobHandler)
		this.manServer.Router().PUT("/v1/webhooks/:appid/:topic/:ver",
			this.manServer.createWebhookHandler)
		this.manServer.Router().GET("/v1/schemas/:appid/:topic/:ver",
			m(this.manServer.schemaHandler))
		this.manServer.Router().DELETE("/v1/manager/cache",
			m(this.manServer.refreshManagerHandler))
	}

	if this.pubServer != nil {
		this.pubServer.Router().GET("/alive", m(this.checkAliveHandler))
		this.pubServer.Router().NotFound = http.HandlerFunc(this.pubServer.notFoundHandler)

		this.pubServer.Router().POST("/v1/raw/msgs/:cluster/:topic", m(this.pubServer.pubRawHandler))
		this.pubServer.Router().POST("/v1/msgs/:topic/:ver", m(this.pubServer.pubHandler))
		this.pubServer.Router().POST("/v1/ws/msgs/:topic/:ver", m(this.pubServer.pubWsHandler))
		this.pubServer.Router().POST("/v1/jobs/:topic/:ver", m(this.pubServer.addJobHandler))
		this.pubServer.Router().DELETE("/v1/jobs/:topic/:ver", m(this.pubServer.deleteJobHandler))

		// TODO deprecated
		this.pubServer.Router().POST("/topics/:topic/:ver", m(this.pubServer.pubHandler))
	}

	if this.subServer != nil {
		this.subServer.Router().GET("/alive", m(this.checkAliveHandler))
		this.subServer.Router().NotFound = http.HandlerFunc(this.subServer.notFoundHandler)

		this.subServer.Router().GET("/v1/msgs/:appid/:topic/:ver", m(this.subServer.subHandler))
		this.subServer.Router().GET("/v1/ws/msgs/:appid/:topic/:ver", m(this.subServer.subWsHandler))
		this.subServer.Router().PUT("/v1/bury/:appid/:topic/:ver", m(this.subServer.buryHandler))
		this.subServer.Router().PUT("/v1/offsets/:appid/:topic/:ver/:group", m(this.subServer.ackHandler))

		// api for pubsub manager
		this.subServer.Router().GET("/v1/raw/msgs/:appid/:topic/:ver", m(this.subServer.subRawHandler))
		this.subServer.Router().GET("/v1/peek/:appid/:topic/:ver", m(this.subServer.peekHandler))
		this.subServer.Router().POST("/v1/shadow/:appid/:topic/:ver/:group", m(this.subServer.addTopicShadowHandler))
		this.subServer.Router().GET("/v1/subd/:topic/:ver", m(this.subServer.subdStatusHandler))
		this.subServer.Router().GET("/v1/status/:appid/:topic/:ver", m(this.subServer.subStatusHandler))
		this.subServer.Router().DELETE("/v1/groups/:appid/:topic/:ver/:group", m(this.subServer.delSubGroupHandler))
		this.subServer.Router().PUT("/v1/offset/:appid/:topic/:ver/:group/:partition", m(this.subServer.resetSubOffsetHandler))

		// TODO deprecated
		this.subServer.Router().GET("/topics/:appid/:topic/:ver", m(this.subServer.subHandler))
	}

	if this.debugMux != nil {
		this.debugMux.HandleFunc("/debug/pprof/", http.HandlerFunc(pprof.Index))
		this.debugMux.HandleFunc("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
		this.debugMux.HandleFunc("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
		this.debugMux.HandleFunc("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
		this.debugMux.HandleFunc("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

		go http.ListenAndServe(Options.DebugHttpAddr, gziphandler.GzipHandler(this.debugMux))

		log.Info("debug http server ready on %s", Options.DebugHttpAddr)
	}

}

func (this *Gateway) checkAliveHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	w.Write(ResponseOk)
}
