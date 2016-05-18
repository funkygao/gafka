package gateway

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (this *Gateway) buildRouting() {
	m := this.MiddlewareKateway

	if this.manServer != nil {
		this.manServer.Router().GET("/alive", m(this.checkAliveHandler))
		this.manServer.Router().NotFound = http.HandlerFunc(this.manServer.notFoundHandler)

		// api for 'gk kateway'
		this.manServer.Router().GET("/v1/clusters", m(this.manServer.clustersHandler))
		this.manServer.Router().GET("/v1/clients", m(this.manServer.clientsHandler))
		this.manServer.Router().GET("/v1/status", m(this.manServer.statusHandler))
		this.manServer.Router().PUT("/v1/options/:option/:value", m(this.manServer.setOptionHandler))
		this.manServer.Router().PUT("/v1/log/:level", m(this.manServer.setlogHandler))
		this.manServer.Router().DELETE("/v1/counter/:name", m(this.manServer.resetCounterHandler))

		// api for pubsub manager
		this.manServer.Router().GET("/v1/partitions/:cluster/:appid/:topic/:ver",
			m(this.manServer.partitionsHandler))
		this.manServer.Router().POST("/v1/topics/:cluster/:appid/:topic/:ver",
			m(this.manServer.addTopicHandler))
		this.manServer.Router().PUT("/v1/topics/:cluster/:appid/:topic/:ver",
			m(this.manServer.updateTopicHandler))
	}

	if this.pubServer != nil {
		this.pubServer.Router().GET("/alive", m(this.checkAliveHandler))
		this.pubServer.Router().NotFound = http.HandlerFunc(this.pubServer.notFoundHandler)

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

}

func (this *Gateway) checkAliveHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	w.Write(ResponseOk)
}
