package swf

func (this *Swf) setupApis() {
	m := this.Middleware

	if this.apiServer != nil {
		this.apiServer.Router().GET("/alive", m(this.apiServer.checkAliveHandler))
	}

}
