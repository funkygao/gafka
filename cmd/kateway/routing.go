package main

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (this *Gateway) buildRouting() {
	m := this.MiddlewareKateway
	this.manServer.Router().GET("/alive", m(this.checkAliveHandler))
	this.manServer.Router().GET("/clusters", m(this.clustersHandler))
	this.manServer.Router().GET("/clients", m(this.clientsHandler))
	this.manServer.Router().GET("/status", m(this.statusHandler))
	this.manServer.Router().PUT("/options/:option/:value", m(this.setOptionHandler))
	this.manServer.Router().PUT("/log/:level", m(this.setlogHandler))
	this.manServer.Router().DELETE("/counter/:name", m(this.resetCounterHandler))

	// api for pubsub manager
	this.manServer.Router().GET("/partitions/:cluster/:appid/:topic/:ver", m(this.partitionsHandler))
	this.manServer.Router().POST("/topics/:cluster/:appid/:topic/:ver", m(this.addTopicHandler))

	if this.pubServer != nil {
		this.pubServer.Router().GET("/raw/topics/:topic/:ver", m(this.pubRawHandler))
		this.pubServer.Router().POST("/topics/:topic/:ver", m(this.pubHandler))
		this.pubServer.Router().POST("/ws/topics/:topic/:ver", m(this.pubWsHandler))
		this.pubServer.Router().GET("/alive", m(this.checkAliveHandler))
	}

	if this.subServer != nil {
		this.subServer.Router().GET("/raw/topics/:appid/:topic/:ver", m(this.subRawHandler))
		this.subServer.Router().GET("/topics/:appid/:topic/:ver", m(this.subHandler))
		this.subServer.Router().GET("/ws/topics/:appid/:topic/:ver", m(this.subWsHandler))
		this.subServer.Router().GET("/alive", m(this.checkAliveHandler))

		// api for pubsub manager
		this.subServer.Router().POST("/guard/:appid/:topic/:ver/:group", m(this.guardTopicHandler))
		this.subServer.Router().GET("/subd/:topic/:ver", m(this.subdStatusHandler))
		this.subServer.Router().GET("/status/:appid/:topic/:ver", m(this.subStatusHandler))
		this.subServer.Router().DELETE("/groups/:appid/:topic/:ver/:group", m(this.delSubGroupHandler))
	}

}

func (this *Gateway) checkAliveHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	w.Write(ResponseOk)
}
