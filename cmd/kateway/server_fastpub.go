// +build fasthttp

package main

import (
	golog "log"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/buaazp/fasthttprouter"
	log "github.com/funkygao/log4go"
	"github.com/valyala/fasthttp"
)

type pubServer struct {
	name string
	gw   *Gateway

	httpAddr     string
	httpListener net.Listener
	httpServer   *fasthttp.Server

	tlsListener net.Listener
	httpsServer *http.Server

	router *fasthttprouter.Router

	waitExitFunc waitExitFunc

	once sync.Once
}

func newPubServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *pubServer {
	this := &pubServer{
		name:     "fastpub",
		gw:       gw,
		httpAddr: httpAddr,
		router:   fasthttprouter.New(),
	}
	this.waitExitFunc = this.waitExit

	if httpAddr != "" {
		logger := golog.New(os.Stdout, "fasthttp ", golog.LstdFlags|golog.Lshortfile)
		this.httpServer = &fasthttp.Server{
			Name:                 "kateway",
			Concurrency:          maxClients,
			MaxConnsPerIP:        5000, // TODO
			MaxRequestsPerConn:   0,    // unlimited
			MaxKeepaliveDuration: httpReadTimeout,
			ReadTimeout:          httpReadTimeout,
			WriteTimeout:         httpWriteTimeout,
			MaxRequestBodySize:   int(options.maxPubSize + 1),
			ReduceMemoryUsage:    false, // TODO
			Handler:              this.router.Handler,
			Logger:               logger,
		}
	}

	return this
}

func (this *pubServer) Start() {
	var err error
	var waitHttpListenerUp chan struct{}
	if this.httpServer != nil {
		waitHttpListenerUp = make(chan struct{})

		go func() {
			if options.cpuAffinity {
				runtime.LockOSThread()
			}

			this.httpListener, err = net.Listen("tcp", this.httpAddr)
			if err != nil {
				panic(err)
			}
			this.httpListener = FastListener(this.httpListener, this.gw)
			close(waitHttpListenerUp)

			var retryDelay time.Duration
			for {
				err = this.httpServer.Serve(this.httpListener)

				// backoff
				if retryDelay == 0 {
					retryDelay = 5 * time.Millisecond
				} else {
					retryDelay = 2 * retryDelay
				}
				if maxDelay := time.Second; retryDelay > maxDelay {
					retryDelay = maxDelay
				}
				log.Error("fastpub server: %v, retry in %v", err, retryDelay)
				time.Sleep(retryDelay)
			}
		}()

		if waitHttpListenerUp != nil {
			<-waitHttpListenerUp
		}
		this.once.Do(func() {
			go this.waitExitFunc(this.gw.shutdownCh)
		})

		this.gw.wg.Add(1)
		log.Info("%s http server ready on %s", this.name, this.httpAddr)
	}

}

func (this *pubServer) Router() *fasthttprouter.Router {
	return this.router
}

func (this *pubServer) waitExit(exit <-chan struct{}) {
	select {
	case <-exit:
		if this.httpServer != nil {
			// HTTP response will have "Connection: close"
			this.httpServer.ReadTimeout = time.Millisecond
			this.httpServer.MaxKeepaliveDuration = time.Millisecond
			this.httpServer.MaxRequestsPerConn = 1

			// avoid new connections
			if err := this.httpListener.Close(); err != nil {
				log.Error(err.Error())
			}

			this.gw.wg.Done()
			log.Trace("%s http server stopped", this.name)
		}

		if this.httpsServer != nil {
			this.gw.wg.Done()
			log.Trace("%s https server stopped", this.name)
		}

	}

}
