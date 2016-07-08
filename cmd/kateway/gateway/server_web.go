package gateway

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

	onStop   func()
	onceStop sync.Once

	// FIXME if http/https listener both enabled, must able to tell them apart
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

func (this *webServer) Start() {
	if this.waitExitFunc == nil {
		this.waitExitFunc = this.waitExit
	}
	if this.connStateFunc == nil {
		this.connStateFunc = this.connStateHandler
	}

	if this.httpsServer != nil {
		this.httpsServer.ConnState = this.connStateFunc
		this.startServer(true)
	}

	if this.httpServer != nil {
		this.httpServer.ConnState = this.connStateFunc
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
		if Options.CpuAffinity {
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
				log.Error("%s listener %v, retry in %v", this.name, err, retryDelay)
				time.Sleep(retryDelay)
				continue
			}

			theListener = LimitListener(this.name, this.gw, theListener, this.maxClients)
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

	case http.StateIdle:
		// TODO track the idle conns, so as to close it when shutdown

	case http.StateActive:
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

	log.Trace("%s on %s listener closed", this.name, server.Addr)

	waitStart := time.Now()
	var prompt sync.Once
	for {
		activeConnN := atomic.LoadInt32(&this.activeConnN)
		if activeConnN == 0 {
			// good luck, all connections finished
			break
		}

		prompt.Do(func() {
			log.Trace("%s on %s waiting for %d clients shutdown...",
				this.name, server.Addr, activeConnN)
		})

		// timeout mechanism
		if time.Since(waitStart) > Options.SubTimeout+time.Second {
			log.Warn("%s on %s still left %d conns after %s, forced to shutdown",
				this.name, server.Addr, activeConnN, Options.SubTimeout)
			break
		}

		time.Sleep(time.Millisecond * 50)
	}
	log.Trace("%s on %s all connections finished", this.name, server.Addr)

	if this.onStop != nil {
		// if both http and https, without sync once, onStop will be called twice
		this.onceStop.Do(func() {
			this.onStop()
		})
	}

	this.gw.wg.Done()
}

func (this *webServer) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	log.Error("%s: not found %s", this.name, r.RequestURI)

	writeNotFound(w)
}
