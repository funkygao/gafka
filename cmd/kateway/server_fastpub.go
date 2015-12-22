// +build fasthttp

package main

import (
	"io/ioutil"
	golog "log"
	"net"
	"net/http"
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
		logger := &golog.Logger{}
		logger.SetOutput(ioutil.Discard)
		this.httpServer = &fasthttp.Server{
			Name:               "kateway",
			Handler:            this.router.Handler,
			Concurrency:        maxClients,
			MaxConnsPerIP:      100,
			MaxRequestBodySize: int(options.maxPubSize + 1),
			ReduceMemoryUsage:  true,
			Logger:             logger,
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
			this.httpListener, err = net.Listen("tcp", this.httpAddr)
			if err != nil {
				panic(err)
			}
			close(waitHttpListenerUp)

			var retryDelay time.Duration
			for {
				select {
				case <-this.gw.shutdownCh:
					return

				default:
				}

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
			this.httpServer.MaxKeepaliveDuration = time.Nanosecond
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
