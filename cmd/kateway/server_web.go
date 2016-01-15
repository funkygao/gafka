package main

import (
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
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

	waitExitFunc    waitExitFunc
	connStateFunc   connStateFunc
	onConnNewFunc   onConnNewFunc
	onConnCloseFunc onConnCloseFunc

	activeConnN int32
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
			Addr:           httpsAddr,
			Handler:        this.router,
			ReadTimeout:    httpReadTimeout,
			WriteTimeout:   httpWriteTimeout,
			MaxHeaderBytes: httpHeaderMaxBytes,
		}
	}

	return this
}

func (this *webServer) Start() {
	if this.waitExitFunc == nil {
		this.waitExitFunc = this.waitExit
	}
	if this.connStateFunc == nil {
		this.connStateFunc = this.connStateHandler
	}

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
	waitListenerUp := make(chan struct{})
	go func() {
		if options.CpuAffinity {
			runtime.LockOSThread()
		}

		var (
			retryDelay         time.Duration
			theListener        net.Listener
			waitListenerUpOnce sync.Once
			addr               string
		)
		for {
			if https {
				this.httpsListener, err = net.Listen("tcp", this.httpsServer.Addr)
				if err != nil {
					if retryDelay == 0 {
						retryDelay = 50 * time.Millisecond
					} else {
						retryDelay = 2 * retryDelay
					}
					if maxDelay := time.Second; retryDelay > maxDelay {
						retryDelay = maxDelay
					}
					log.Error("%s listener %v, retry in %v", this.name, err, retryDelay)
					time.Sleep(retryDelay)
					continue
				}

				this.httpsListener, err = setupHttpsListener(this.httpsListener,
					this.gw.certFile, this.gw.keyFile)
				if err != nil {
					panic(err)
				}

				theListener = this.httpsListener
				addr = this.httpsServer.Addr
			} else {
				this.httpListener, err = net.Listen("tcp", this.httpServer.Addr)
				theListener = this.httpListener
				addr = this.httpServer.Addr
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
				log.Error("%s listener %v, retry in %v", this.name, err, retryDelay)
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

			select {
			case <-this.gw.shutdownCh:
				log.Trace("%s server stopped on %s", this.name, addr)
				return

			default:
				log.Error("%s server: %v", this.name, err)
			}
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

func (this *webServer) connStateHandler(c net.Conn, cs http.ConnState) {
	switch cs {
	case http.StateNew:
		atomic.AddInt32(&this.activeConnN, 1)

		if this.onConnNewFunc != nil {
			this.onConnNewFunc(c)
		}

	case http.StateActive, http.StateIdle:
		// do nothing by default

	case http.StateClosed, http.StateHijacked:
		atomic.AddInt32(&this.activeConnN, -1)

		if this.onConnCloseFunc != nil {
			this.onConnCloseFunc(c)
		}

	}
}

func (this *webServer) waitExit(server *http.Server, listener net.Listener, exit <-chan struct{}) {
	<-exit

	// HTTP response will have "Connection: close"
	server.SetKeepAlivesEnabled(false)

	// avoid new connections
	if err := listener.Close(); err != nil {
		log.Error(err.Error())
	}

	// wait for active connections finish up to 4s
	const maxWaitSeconds = 4
	waitStart := time.Now()
	for {
		activeConnN := atomic.LoadInt32(&this.activeConnN)
		if activeConnN == 0 {
			// good luck, all connections finished
			break
		}

		// timeout mechanism
		if time.Since(waitStart).Seconds() > maxWaitSeconds {
			log.Warn("%s on %s still left %d conns, but forced to shutdown after %ss",
				this.name, server.Addr, activeConnN, maxWaitSeconds)
			break
		}

		time.Sleep(time.Millisecond * 50)
	}
	log.Trace("%s on %s all connections finished", this.name, server.Addr)

	this.gw.wg.Done()
}
