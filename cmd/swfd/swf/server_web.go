package swf

import (
	"net"
	"net/http"

	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

type webServer struct {
	name string

	router *httprouter.Router

	httpListener net.Listener
	httpServer   *http.Server

	httpsListener net.Listener
	httpsServer   *http.Server
}

func newWebServer(name string, httpAddr, httpsAddr string) *webServer {
	this := &webServer{
		name:   name,
		router: httprouter.New(),
	}

	if Options.EnableHttpPanicRecover {
		this.router.PanicHandler = func(w http.ResponseWriter,
			r *http.Request, err interface{}) {
			log.Error("%s[%s] %s %s: %+v", this.name, r.RemoteAddr, r.Method, r.RequestURI, err)
		}
	}

	if httpAddr != "" {
		this.httpServer = &http.Server{
			Addr:           httpAddr,
			Handler:        this.router,
			ReadTimeout:    Options.HttpReadTimeout,
			WriteTimeout:   Options.HttpWriteTimeout,
			MaxHeaderBytes: Options.HttpHeaderMaxBytes,
		}
	}

	if httpsAddr != "" {
		this.httpsServer = &http.Server{
			Addr:           httpsAddr,
			Handler:        this.router,
			ReadTimeout:    Options.HttpReadTimeout,
			WriteTimeout:   Options.HttpWriteTimeout,
			MaxHeaderBytes: Options.HttpHeaderMaxBytes,
		}
	}

	return this
}

func (this *webServer) Name() string {
	return this.name
}

func (this *webServer) Router() *httprouter.Router {
	return this.router
}

func (this *webServer) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	log.Error("%s: not found %s", this.name, r.RequestURI)

	w.Header().Set("Connection", "close")
	http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func (this *webServer) checkAliveHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.Write(ResponseOk)
}

func (this *webServer) NotImplemented(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
}
