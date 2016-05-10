package gateway

// management server
type manServer struct {
	*webServer
}

func newManServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *manServer {
	this := &manServer{
		webServer: newWebServer("man", httpAddr, httpsAddr, maxClients, gw),
	}

	return this
}
