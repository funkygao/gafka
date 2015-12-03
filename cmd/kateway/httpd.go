package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"time"

	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

type waitExitFunc func(exit <-chan struct{})

type webServer struct {
	name       string
	maxClients int
	gw         *Gateway

	listener net.Listener
	server   *http.Server

	httpsServer *http.Server
	tlsListener net.Listener

	router *mux.Router

	waitExitFunc waitExitFunc

	once sync.Once
}

func newWebServer(name string, httpAddr, httpsAddr string, maxClients int,
	gw *Gateway) *webServer {
	this := &webServer{
		name:       name,
		router:     mux.NewRouter(),
		gw:         gw,
		maxClients: maxClients,
	}

	if httpAddr != "" {
		this.server = &http.Server{
			Addr:           httpAddr,
			Handler:        this.router,
			ReadTimeout:    time.Minute, // FIXME
			WriteTimeout:   time.Minute, // FIXME
			MaxHeaderBytes: 4 << 10,     // should be enough
		}
	}

	if httpsAddr != "" {
		this.httpsServer = &http.Server{
			Addr:           httpAddr,
			Handler:        this.router,
			ReadTimeout:    0,       // FIXME
			WriteTimeout:   0,       // FIXME
			MaxHeaderBytes: 4 << 10, // should be enough
		}
	}

	return this
}

func (this *webServer) Start() {
	var err error
	if this.server != nil {
		this.listener, err = net.Listen("tcp", this.server.Addr)
		if err != nil {
			panic(err)
		}

		this.listener = LimitListener(this.listener, this.maxClients)
		go this.server.Serve(this.listener)

		this.once.Do(func() {
			go this.waitExitFunc(this.gw.shutdownCh)
		})

		this.gw.wg.Add(1)
		log.Info("%s http server ready on %s", this.name, this.server.Addr)
	}

	if this.httpsServer != nil {
		this.tlsListener, err = this.setupHttpsServer(this.httpsServer,
			this.gw.certFile, this.gw.keyFile)
		if err != nil {
			panic(err)
		}

		this.tlsListener = LimitListener(this.tlsListener, this.maxClients)
		go this.httpsServer.Serve(this.tlsListener)

		this.once.Do(func() {
			go this.waitExitFunc(this.gw.shutdownCh)
		})

		this.gw.wg.Add(1)
		log.Info("%s https server ready on %s", this.name, this.server.Addr)
	}

}

func (this *webServer) Router() *mux.Router {
	return this.router
}

func (this *webServer) setupHttpsServer(server *http.Server, certFile, keyFile string) (net.Listener, error) {
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return nil, err
	}

	config := &tls.Config{}
	config.NextProtos = []string{"http/1.1"}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	tlsListener := tls.NewListener(listener, config)
	return tlsListener, nil
}
