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
	var waitHttpListenerUp chan struct{}
	if this.httpServer != nil {
		waitHttpListenerUp = make(chan struct{})

		go func() {
			var retryDelay time.Duration
			for {
				select {
				case <-this.gw.shutdownCh:
					return

				default:
				}

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

				this.httpListener = LimitListener(this.gw, this.httpListener, this.maxClients)
				close(waitHttpListenerUp)

				err = this.httpServer.Serve(this.httpListener)
				log.Error("%s: %v", this.name, err)
			}
		}()

		// FIXME if net.Listen fails, kateway will not be able to stop
		// e,g. start a kateway, then start another, the 2nd will not be able to stop
		if waitHttpListenerUp != nil {
			<-waitHttpListenerUp
		}
		this.once.Do(func() {
			go this.waitExitFunc(this.gw.shutdownCh)
		})

		this.gw.wg.Add(1)
		log.Info("%s http server ready on %s", this.name, this.httpServer.Addr)
	}

	var waitHttpsListenerUp chan struct{}
	if this.httpsServer != nil {
		this.tlsListener, err = this.setupHttpsServer(this.httpsServer,
			this.gw.certFile, this.gw.keyFile)
		if err != nil {
			panic(err)
		}

		go func() {
			var retryDelay time.Duration
			for {
				select {
				case <-this.gw.shutdownCh:
					return

				default:
				}

				this.tlsListener, err = net.Listen("tcp", this.httpsServer.Addr)
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

				this.tlsListener = LimitListener(this.gw, this.tlsListener, this.maxClients)
				close(waitHttpsListenerUp)

				err = this.httpsServer.Serve(this.tlsListener)
				log.Error("%s: %v", this.name, err)
			}
		}()

		if this.httpsServer != nil {
			<-waitHttpsListenerUp
		}
		this.once.Do(func() {
			go this.waitExitFunc(this.gw.shutdownCh)
		})

		this.gw.wg.Add(1)
		log.Info("%s https server ready on %s", this.name, this.httpsServer.Addr)
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
