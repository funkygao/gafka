package main

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (this *Gateway) buildRouting() {
	this.manServer.Router().GET("/alive", this.checkAliveHandler)
	this.manServer.Router().GET("/clusters", this.clustersHandler)
	this.manServer.Router().GET("/clients", this.clientsHandler)
	this.manServer.Router().GET("/help", this.helpHandler)
	this.manServer.Router().GET("/status", this.statusHandler)
	this.manServer.Router().PUT("/options/:option/:value", this.setOptionHandler)
	this.manServer.Router().PUT("/log/:level", this.setlogHandler)
	this.manServer.Router().GET("/partitions/:cluster/:appid/:topic/:ver", this.partitionsHandler)
	this.manServer.Router().POST("/topics/:cluster/:appid/:topic/:ver", this.addTopicHandler)
	this.manServer.Router().DELETE("/counter/:name", this.resetCounterHandler)

	if this.pubServer != nil {
		this.pubServer.Router().GET("/raw/topics/:topic/:ver", this.pubRawHandler)
		this.pubServer.Router().POST("/topics/:topic/:ver", this.pubHandler)
		this.pubServer.Router().POST("/ws/topics/:topic/:ver", this.pubWsHandler)
		this.pubServer.Router().GET("/alive", this.checkAliveHandler)
	}

	if this.subServer != nil {
		this.subServer.Router().GET("/raw/topics/:appid/:topic/:ver", this.subRawHandler)
		this.subServer.Router().GET("/topics/:appid/:topic/:ver", this.subHandler)
		this.subServer.Router().GET("/ws/topics/:appid/:topic/:ver", this.subWsHandler)
		this.subServer.Router().GET("/alive", this.checkAliveHandler)
	}

}

func (this *Gateway) checkAliveHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)
	w.Write(ResponseOk)
}
