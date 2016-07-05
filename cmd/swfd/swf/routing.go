package swf

import (
	"net/http"
)

func (this *Swf) setupApis() {
	m := this.Middleware

	if this.apiServer != nil {
		this.apiServer.Router().GET("/alive", m(this.apiServer.checkAliveHandler))
		this.apiServer.Router().NotFound = http.HandlerFunc(this.apiServer.notFoundHandler)
		this.apiServer.Router().POST("/v1", m(this.apiServer.mainEntryPoint))
	}

}
