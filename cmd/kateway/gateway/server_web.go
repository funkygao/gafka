package gateway

import (
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
)

type webServer struct {
	gw *Gateway

	name       string
	maxClients int

	router *httprouter.Router

	httpListener net.Listener
	httpServer   *http.Server

	httpsListener net.Listener
	httpsServer   *http.Server

	waitExitFunc    waitExitFunc
	connStateFunc   connStateFunc
	onConnNewFunc   onConnNewFunc
	onConnCloseFunc onConnCloseFunc

	onStop        func()
	mu            sync.Mutex
	waiterStarted bool

	// FIXME if http/https listener both enabled, must able to tell them apart
	activeConnN int32

	closed chan struct{}
}

func newWebServer(name string, httpAddr, httpsAddr string, maxClients int, gw *Gateway) *webServer {
	this := &webServer{
		name:       name,
		gw:         gw,
		maxClients: maxClients,
		router:     httprouter.New(),
		closed:     make(chan struct{}),
	}

	if Options.EnableHttpPanicRecover {
		this.router.PanicHandler = func(w http.ResponseWriter, r *http.Request, err interface{}) {
			log.Error("PANIC %s %s(%s) %s %s: %+v", this.name, r.RemoteAddr, getHttpRemoteIp(r), r.Method, r.RequestURI, err)

			writeServerError(w, http.StatusText(http.StatusInternalServerError))
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

func (this *webServer) Router() *httprouter.Router {
	return this.router
}

func (this *webServer) Start() {
	if this.waitExitFunc == nil {
		this.waitExitFunc = this.defaultWaitExit
	}
	if this.connStateFunc == nil {
		this.connStateFunc = this.defaultConnStateMachine
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
					if strings.HasSuffix(err.Error(), "address already in use") {
						// non-retriable error encountered
						panic(fmt.Errorf("%s listener: %v", this.name, err))
					}

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

				theListener, err = setupHttpsListener(this.httpsListener, this.gw.certFile, this.gw.keyFile)
				if err != nil {
					panic(err)
				}
			} else {
				theListener, err = net.Listen("tcp", this.httpServer.Addr)
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
				this.mu.Lock()
				this.httpsListener = theListener
				this.mu.Unlock()

				err = this.httpsServer.Serve(theListener)
			} else {
				this.mu.Lock()
				this.httpListener = theListener
				this.mu.Unlock()

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

	// wait for the listener up
	<-waitListenerUp

	this.mu.Lock()
	if !this.waiterStarted {
		this.waiterStarted = true

		this.gw.wg.Add(1)
		go this.waitExitFunc(this.gw.shutdownCh)
	}
	this.mu.Unlock()

	if https {
		log.Info("%s https server ready on %s", this.name, this.httpsServer.Addr)
	} else {
		log.Info("%s http server ready on %s", this.name, this.httpServer.Addr)
	}
}

func (this *webServer) defaultConnStateMachine(c net.Conn, cs http.ConnState) {
	switch cs {
	case http.StateNew:
		atomic.AddInt32(&this.activeConnN, 1)

		if this.onConnNewFunc != nil {
			this.onConnNewFunc(c)
		}

	case http.StateIdle:

	case http.StateActive:

	case http.StateClosed, http.StateHijacked:
		atomic.AddInt32(&this.activeConnN, -1)

		if this.onConnCloseFunc != nil {
			this.onConnCloseFunc(c)
		}
	}
}

func (this *webServer) defaultWaitExit(exit <-chan struct{}) {
	log.Debug("%s enter default wait exit", this.name)

	<-exit

	var err error
	if this.httpServer != nil {
		// HTTP response will have "Connection: close"
		this.httpServer.SetKeepAlivesEnabled(false)

		// avoid new connections
		this.mu.Lock()
		if this.httpListener != nil {
			err = this.httpListener.Close()
		}
		this.mu.Unlock()

		if err != nil {
			log.Error("%s on %s: %+v", this.name, this.httpServer.Addr, err)
		}

		log.Trace("%s on %s listener closed", this.name, this.httpServer.Addr)
	}

	if this.httpsServer != nil {
		// HTTP response will have "Connection: close"
		this.httpsServer.SetKeepAlivesEnabled(false)

		// avoid new connections
		this.mu.Lock()
		if this.httpsListener != nil {
			err = this.httpsListener.Close()
		}
		this.mu.Unlock()

		if err != nil {
			log.Error("%s on %s: %+v", this.name, this.httpsServer.Addr, err)
		}

		log.Trace("%s on %s listener closed", this.name, this.httpsServer.Addr)
	}

	// wait for all established http/https conns close
	waitStart := time.Now()
	var prompt sync.Once
	for {
		activeConnN := atomic.LoadInt32(&this.activeConnN)
		if activeConnN == 0 {
			// good luck, all connections finished
			break
		}

		prompt.Do(func() {
			log.Trace("%s waiting for %d clients shutdown...", this.name, activeConnN)
		})

		// timeout mechanism
		if time.Since(waitStart) > time.Second {
			log.Warn("%s still left %d conns, will be forced to shutdown", this.name, activeConnN)
			break
		}

		time.Sleep(time.Millisecond * 50)
	}
	log.Trace("%s all connections finished", this.name)

	if this.httpsServer != nil {
		this.httpsServer.ConnState = nil
	}
	if this.httpServer != nil {
		this.httpServer.ConnState = nil
	}

	if this.onStop != nil {
		this.onStop()
	}

	this.gw.wg.Done()
	close(this.closed)
}

func (this *webServer) Closed() <-chan struct{} {
	return this.closed
}

func (this *webServer) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	log.Error("%s: %s(%s) 404 %s %s ", this.name,
		r.RemoteAddr, getHttpRemoteIp(r),
		r.Method, r.RequestURI)

	writeNotFound(w)
}

func (this *webServer) notAllowedHandler(w http.ResponseWriter, r *http.Request) {
	log.Error("%s: %s(%s) 405 %s %s ", this.name,
		r.RemoteAddr, getHttpRemoteIp(r),
		r.Method, r.RequestURI)

	writeNotAllowed(w)
}
