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

	// TODO channel performance is frustrating, no better than mutex/map use ring buffer
	stateIdleCh, stateRemoveCh, stateActiveCh chan net.Conn
}

func newWebServer(name string, httpAddr, httpsAddr string, maxClients int,
	gw *Gateway) *webServer {
	const initialConnBuckets = 200
	this := &webServer{
		name:          name,
		gw:            gw,
		maxClients:    maxClients,
		stateActiveCh: make(chan net.Conn, initialConnBuckets),
		stateIdleCh:   make(chan net.Conn, initialConnBuckets),
		stateRemoveCh: make(chan net.Conn, initialConnBuckets),
		router:        httprouter.New(),
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

func (this *webServer) Router() *httprouter.Router {
	return this.router
}

func (this *webServer) Start() {
	if this.waitExitFunc == nil {
		this.waitExitFunc = this.defaultWaitExit
	}
	if this.connStateFunc == nil {
		this.connStateFunc = this.defaultConnStateMachine

		go this.manageIdleConns()
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
		this.stateIdleCh <- c

	case http.StateActive:
		this.stateActiveCh <- c

	case http.StateClosed, http.StateHijacked:
		atomic.AddInt32(&this.activeConnN, -1)
		this.stateRemoveCh <- c

		if this.onConnCloseFunc != nil {
			this.onConnCloseFunc(c)
		}
	}
}

func (this *webServer) manageIdleConns() {
	var (
		idleConns     = make(map[net.Conn]struct{}, 200)
		c             net.Conn
		waitNextRound = make(chan struct{}, 10)
	)
	defer close(waitNextRound)

	log.Debug("%s is managing idle connections", this.name)

	for {
		select {
		case <-this.gw.shutdownCh:
			if len(idleConns) == 0 {
				// happy ending
				log.Debug("%s closed all idle conns", this.name)
				return
			}

			t := time.Now().Add(time.Millisecond * 100)
			for conn := range idleConns {
				if conn == nil {
					continue
				}

				log.Debug("%s closing %s", this.name, conn.RemoteAddr())
				conn.SetDeadline(t)
			}

			// wait for next loop
			waitNextRound <- struct{}{}

		case <-waitNextRound:
			if len(idleConns) == 0 {
				log.Debug("%s closed all idle conns", this.name)
				return
			}

			t := time.Now().Add(time.Millisecond * 100)
			for conn := range idleConns {
				if conn == nil {
					continue
				}

				log.Debug("%s closing %s", this.name, conn.RemoteAddr())
				conn.SetDeadline(t)
			}

			// wait for next loop
			waitNextRound <- struct{}{}

		case c = <-this.stateActiveCh:
			delete(idleConns, c)

		case c = <-this.stateIdleCh:
			idleConns[c] = struct{}{}

		case c = <-this.stateRemoveCh:
			delete(idleConns, c)
		}
	}
}

func (this *webServer) defaultWaitExit(exit <-chan struct{}) {
	log.Debug("%s enter default wait exit", this.name)

	<-exit

	if this.httpServer != nil {
		// HTTP response will have "Connection: close"
		this.httpServer.SetKeepAlivesEnabled(false)

		// avoid new connections
		if err := this.httpListener.Close(); err != nil {
			log.Error(err.Error())
		}

		log.Trace("%s on %s listener closed", this.name, this.httpServer.Addr)
	}

	if this.httpsServer != nil {
		// HTTP response will have "Connection: close"
		this.httpsServer.SetKeepAlivesEnabled(false)

		// avoid new connections
		if err := this.httpsListener.Close(); err != nil {
			log.Error(err.Error())
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
		if time.Since(waitStart) > Options.SubTimeout+time.Second {
			log.Warn("%s still left %d conns after %s, forced to shutdown",
				this.name, activeConnN, Options.SubTimeout)
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

	close(this.stateActiveCh)
	close(this.stateIdleCh)
	close(this.stateRemoveCh)

	if this.onStop != nil {
		this.onStop()
	}

	this.gw.wg.Done()
}

func (this *webServer) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	log.Error("%s: not found %s", this.name, r.RequestURI)

	writeNotFound(w)
}
