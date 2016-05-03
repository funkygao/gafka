package main

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (this *Gateway) buildRouting() {
	m := this.MiddlewareKateway

	if this.manServer != nil {
		this.manServer.Router().GET("/alive", m(this.checkAliveHandler))
		this.manServer.Router().NotFound = http.HandlerFunc(this.notFoundHandler)

		// api for 'gk kateway'
		this.manServer.Router().GET("/v1/clusters", m(this.clustersHandler))
		this.manServer.Router().GET("/v1/clients", m(this.clientsHandler))
		this.manServer.Router().GET("/v1/status", m(this.statusHandler))
		this.manServer.Router().PUT("/v1/options/:option/:value", m(this.setOptionHandler))
		this.manServer.Router().PUT("/v1/log/:level", m(this.setlogHandler))
		this.manServer.Router().DELETE("/v1/counter/:name", m(this.resetCounterHandler))

		// api for pubsub manager
		this.manServer.Router().GET("/v1/partitions/:cluster/:appid/:topic/:ver",
			m(this.partitionsHandler))
		this.manServer.Router().POST("/v1/topics/:cluster/:appid/:topic/:ver",
			m(this.addTopicHandler))
		this.manServer.Router().PUT("/v1/topics/:cluster/:appid/:topic/:ver",
			m(this.updateTopicHandler))
	}

	if this.pubServer != nil {
		this.pubServer.Router().GET("/alive", m(this.checkAliveHandler))
		this.pubServer.Router().NotFound = http.HandlerFunc(this.notFoundHandler)

		// TODO /v1/msgs/:topic/:ver
		this.pubServer.Router().POST("/v1/msgs/:topic/:ver", m(this.pubHandler))
		this.pubServer.Router().POST("/v1/ws/msgs/:topic/:ver", m(this.pubWsHandler))
		this.pubServer.Router().POST("/v1/jobs/:topic/:ver", m(this.addJobHandler))
		this.pubServer.Router().DELETE("/v1/jobs/:topic/:ver", m(this.deleteJobHandler))

		// TODO deprecated
		this.pubServer.Router().POST("/topics/:topic/:ver", m(this.pubHandler))
	}

	if this.subServer != nil {
		this.subServer.Router().GET("/alive", m(this.checkAliveHandler))
		this.subServer.Router().NotFound = http.HandlerFunc(this.notFoundHandler)

		this.subServer.Router().GET("/v1/msgs/:appid/:topic/:ver", m(this.subHandler))
		this.subServer.Router().GET("/v1/ws/msgs/:appid/:topic/:ver", m(this.subWsHandler))
		this.subServer.Router().PUT("/v1/bury/:appid/:topic/:ver", m(this.buryHandler))

		// api for pubsub manager
		this.subServer.Router().GET("/v1/raw/msgs/:appid/:topic/:ver", m(this.subRawHandler))
		this.subServer.Router().GET("/v1/peek/:appid/:topic/:ver", m(this.peekHandler))
		this.subServer.Router().POST("/v1/shadow/:appid/:topic/:ver/:group", m(this.addTopicShadowHandler))
		this.subServer.Router().GET("/v1/subd/:topic/:ver", m(this.subdStatusHandler))
		this.subServer.Router().GET("/v1/status/:appid/:topic/:ver", m(this.subStatusHandler))
		this.subServer.Router().DELETE("/v1/groups/:appid/:topic/:ver/:group", m(this.delSubGroupHandler))
		this.subServer.Router().PUT("/v1/offset/:appid/:topic/:ver/:group/:partition", m(this.resetSubOffsetHandler))

		// TODO deprecated
		this.subServer.Router().GET("/topics/:appid/:topic/:ver", m(this.subHandler))
	}

}

func (this *Gateway) checkAliveHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	w.Write(ResponseOk)
}
