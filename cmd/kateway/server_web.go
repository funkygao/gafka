package main

import (
	"net"
	"net/http"
	"runtime"
	"sync"
	"time"

	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

type webServer struct {
	name       string
	maxClients int
	gw         *Gateway

	httpListener net.Listener
	httpServer   *http.Server

	httpsListener net.Listener
	httpsServer   *http.Server

	router *httprouter.Router

	waitExitFunc waitExitFunc
}

func newWebServer(name string, httpAddr, httpsAddr string, maxClients int,
	gw *Gateway) *webServer {
	this := &webServer{
		name:       name,
		gw:         gw,
		maxClients: maxClients,
		router:     httprouter.New(),
	}

	if httpAddr != "" {
		this.httpServer = &http.Server{
			Addr:           httpAddr,
			Handler:        this.router,
			ReadTimeout:    httpReadTimeout,
			WriteTimeout:   httpWriteTimeout,
			MaxHeaderBytes: httpHeaderMaxBytes,
		}
	}

	if httpsAddr != "" {
		this.httpsServer = &http.Server{
			Addr:           httpAddr,
			Handler:        this.router,
			ReadTimeout:    httpReadTimeout,
			WriteTimeout:   httpWriteTimeout,
			MaxHeaderBytes: httpHeaderMaxBytes,
		}
	}

	return this
}

func (this *webServer) Start() {
	if this.httpsServer != nil {
		this.startServer(true)
	}

	if this.httpServer != nil {
		this.startServer(false)
	}

}

func (this *webServer) Router() *httprouter.Router {
	return this.router
}

func (this *webServer) startServer(https bool) {
	var err error
	if https {
		this.httpsListener, err = setupHttpsListener(this.httpsServer.Addr,
			this.gw.certFile, this.gw.keyFile)
		if err != nil {
			panic(err)
		}
	}

	waitListenerUp := make(chan struct{})
	go func() {
		if options.cpuAffinity {
			runtime.LockOSThread()
		}

		var (
			retryDelay         time.Duration
			theListener        net.Listener
			waitListenerUpOnce sync.Once
		)
		for {
			if https {
				this.httpsListener, err = net.Listen("tcp", this.httpsServer.Addr)
				theListener = this.httpsListener
			} else {
				this.httpListener, err = net.Listen("tcp", this.httpServer.Addr)
				theListener = this.httpListener
			}

			if err != nil {
				if retryDelay == 0 {
					retryDelay = 50 * time.Millisecond
				} else {
					retryDelay = 2 * retryDelay
				}
				if maxDelay := time.Second; retryDelay > maxDelay {
					retryDelay = maxDelay
				}
				log.Error("listener %v, retry in %v", err, retryDelay)
				time.Sleep(retryDelay)
				continue
			}

			theListener = LimitListener(this.gw, theListener, this.maxClients)
			waitListenerUpOnce.Do(func() {
				close(waitListenerUp)
			})

			// on non-temporary err, net/http will close the listener
			if https {
				err = this.httpsServer.Serve(theListener)
			} else {
				err = this.httpServer.Serve(theListener)
			}
			log.Error("%s server: %v", this.name, err)
		}
	}()

	// FIXME if net.Listen fails, kateway will not be able to stop
	// e,g. start a kateway, then start another, the 2nd will not be able to stop

	// wait for the listener up
	<-waitListenerUp

	this.gw.wg.Add(1)
	if https {
		go this.waitExitFunc(this.httpsServer, this.httpsListener, this.gw.shutdownCh)
	} else {
		go this.waitExitFunc(this.httpServer, this.httpListener, this.gw.shutdownCh)
	}

	if https {
		log.Info("%s https server ready on %s", this.name, this.httpsServer.Addr)
	} else {
		log.Info("%s http server ready on %s", this.name, this.httpServer.Addr)
	}
}
