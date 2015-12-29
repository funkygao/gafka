package main

func (this *Gateway) buildRouting() {
	this.manServer.Router().GET("/clusters", this.clustersHandler)
	this.manServer.Router().GET("/help", this.helpHandler)
	this.manServer.Router().GET("/status", this.statusHandler)
	this.manServer.Router().POST("/topics/:appid/:cluster/:topic/:ver", this.addTopicHandler)

	if this.pubServer != nil {
		this.pubServer.Router().GET("/raw/topics/:topic/:ver", this.pubRawHandler)
		this.pubServer.Router().POST("/topics/:topic/:ver", this.pubHandler)
		this.pubServer.Router().POST("/ws/topics/:topic/:ver", this.pubWsHandler)
	}

	if this.subServer != nil {
		this.subServer.Router().GET("/raw/topics/:appid/:topic/:ver", this.subRawHandler)
		this.subServer.Router().GET("/topics/:appid/:topic/:ver/:group", this.subHandler)
		this.subServer.Router().GET("/ws/topics/:appid/:topic/:ver/:group", this.subWsHandler)
	}

}
