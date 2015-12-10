package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"time"

	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

type waitExitFunc func(exit <-chan struct{})

type webServer struct {
	name       string
	maxClients int
	gw         *Gateway

	httpListener net.Listener
	httpServer   *http.Server

	tlsListener net.Listener
	httpsServer *http.Server

	router *httprouter.Router

	waitExitFunc waitExitFunc

	once sync.Once
}

func newWebServer(name string, httpAddr, httpsAddr string, maxClients int,
	gw *Gateway) *webServer {
	this := &webServer{
		name:       name,
		router:     httprouter.New(),
		gw:         gw,
		maxClients: maxClients,
	}

	if httpAddr != "" {
		this.httpServer = &http.Server{
			Addr:    httpAddr,
			Handler: this.router,
			//ReadTimeout:    time.Minute, // FIXME
			//WriteTimeout:   time.Minute, // FIXME
			MaxHeaderBytes: 4 << 10, // should be enough
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
	if this.httpServer != nil {
		go func() {
			var retryDelay time.Duration
			for {
				this.httpListener, err = net.Listen("tcp", this.httpServer.Addr)
				if err != nil {
					if retryDelay == 0 {
						retryDelay = 5 * time.Millisecond
					} else {
						retryDelay = 2 * retryDelay
					}
					if maxDelay := time.Second; retryDelay > maxDelay {
						retryDelay = maxDelay
					}
					log.Error("%v, retry in %v", err, retryDelay)
					time.Sleep(retryDelay)
					continue
				}

				this.httpListener = LimitListener(this.httpListener, this.maxClients)

				// non temporary net error, I have to die
				log.Error(this.httpServer.Serve(this.httpListener))
			}
		}()

		this.once.Do(func() {
			go this.waitExitFunc(this.gw.shutdownCh)
		})

		this.gw.wg.Add(1)
		log.Info("%s http server ready on %s", this.name, this.httpServer.Addr)
	}

	if this.httpsServer != nil {
		this.tlsListener, err = this.setupHttpsServer(this.httpsServer,
			this.gw.certFile, this.gw.keyFile)
		if err != nil {
			panic(err)
		}

		this.tlsListener = LimitListener(this.tlsListener, this.maxClients)
		go func() {
			err := this.httpsServer.Serve(this.tlsListener)

			// non temporary net error, I have to die
			log.Error("http server: %s", err.Error())
			this.gw.Stop()
		}()

		this.once.Do(func() {
			go this.waitExitFunc(this.gw.shutdownCh)
		})

		this.gw.wg.Add(1)
		log.Info("%s https server ready on %s", this.name, this.httpsServer.Addr)
	}

}

func (this *webServer) startWebServer(l net.Listener, s *http.Server) {
	var retryDelay time.Duration
	var err error
	for {
		l, err = net.Listen("tcp", s.Addr)
		if err != nil {
			if retryDelay == 0 {
				retryDelay = 5 * time.Millisecond
			} else {
				retryDelay = 2 * retryDelay
			}
			if maxDelay := time.Second; retryDelay > maxDelay {
				retryDelay = maxDelay
			}
			log.Error("%v, retry in %v", err, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		l = LimitListener(l, this.maxClients)

		// non temporary net error, I have to die
		log.Error(s.Serve(l))
	}
}

func (this *webServer) Router() *httprouter.Router {
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
