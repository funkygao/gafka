// +build fasthttp

package main

import (
	golog "log"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/buaazp/fasthttprouter"
	log "github.com/funkygao/log4go"
	"github.com/valyala/fasthttp"
)

type myFastServer struct {
	*fasthttp.Server
	Addr string
}

type pubServer struct {
	name string
	gw   *Gateway

	httpListener net.Listener
	httpServer   *myFastServer

	httpsListener net.Listener
	httpsServer   *myFastServer

	router *fasthttprouter.Router
}

func newPubServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *pubServer {
	this := &pubServer{
		name:   "fastpub",
		gw:     gw,
		router: fasthttprouter.New(),
	}

	logger := golog.New(os.Stdout, "fasthttp ", golog.LstdFlags|golog.Lshortfile)
	if httpAddr != "" {
		this.httpServer = &myFastServer{
			Server: &fasthttp.Server{
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
			},
			Addr: httpAddr,
		}
	}

	if httpsAddr != "" {
		this.httpServer = &myFastServer{
			Server: &fasthttp.Server{
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
			},
			Addr: httpsAddr,
		}
	}

	return this
}

func (this *pubServer) Start() {
	if this.httpServer != nil {
		this.startServer(false)
	}

	if this.httpsServer != nil {
		this.startServer(true)
	}

}

func (this *pubServer) Router() *fasthttprouter.Router {
	return this.router
}

func (this *pubServer) startServer(https bool) {
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
			retryDelay  time.Duration
			theListener net.Listener
		)

		if https {
			this.httpsListener, err = net.Listen("tcp", this.httpsServer.Addr)
			theListener = this.httpsListener
		} else {
			this.httpListener, err = net.Listen("tcp", this.httpServer.Addr)
			theListener = this.httpListener
		}
		theListener = FastListener(this.gw, theListener)
		close(waitListenerUp)

		for {
			if https {
				err = this.httpsServer.Serve(this.httpsListener)
			} else {
				err = this.httpServer.Serve(this.httpListener)
			}

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

	// FIXME if net.Listen fails, kateway will not be able to stop
	// e,g. start a kateway, then start another, the 2nd will not be able to stop

	// wait for the listener up
	<-waitListenerUp

	this.gw.wg.Add(1)
	if https {
		go this.waitExit(this.httpsServer, this.httpsListener, this.gw.shutdownCh)
	} else {
		go this.waitExit(this.httpServer, this.httpListener, this.gw.shutdownCh)
	}

	if https {
		log.Info("%s fast https server ready on %s", this.name, this.httpsServer.Addr)
	} else {
		log.Info("%s fast http server ready on %s", this.name, this.httpServer.Addr)
	}

}

func (this *pubServer) waitExit(server *myFastServer, listener net.Listener, exit <-chan struct{}) {
	<-exit

	// HTTP response will have "Connection: close"
	server.ReadTimeout = time.Millisecond
	server.MaxKeepaliveDuration = time.Millisecond
	server.MaxRequestsPerConn = 1

	// avoid new connections
	if err := listener.Close(); err != nil {
		log.Error(err.Error())
	}

	this.gw.wg.Done()
	log.Trace("%s server stopped on %s", this.name, server.Addr)
}
