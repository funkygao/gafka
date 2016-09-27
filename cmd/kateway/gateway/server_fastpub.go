// +build fasthttp

package gateway

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

type fastServerWithAddr struct {
	*fasthttp.Server
	Addr string
}

type pubServer struct {
	name string
	gw   *Gateway

	httpListener net.Listener
	httpServer   *fastServerWithAddr

	httpsListener net.Listener
	httpsServer   *fastServerWithAddr

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
		this.httpServer = &fastServerWithAddr{
			Server: &fasthttp.Server{
				Name:                 "kateway",
				Concurrency:          maxClients,
				MaxConnsPerIP:        5000, // TODO
				MaxRequestsPerConn:   0,    // unlimited
				MaxKeepaliveDuration: options.HttpReadTimeout,
				ReadTimeout:          options.HttpReadTimeout,
				WriteTimeout:         options.HttpWriteTimeout,
				MaxRequestBodySize:   int(options.MaxPubSize + 1),
				ReduceMemoryUsage:    false, // TODO
				Handler:              this.router.Handler,
				Logger:               logger,
			},
			Addr: httpAddr,
		}
	}

	if httpsAddr != "" {
		this.httpsServer = &fastServerWithAddr{
			Server: &fasthttp.Server{
				Name:                 "kateway",
				Concurrency:          maxClients,
				MaxConnsPerIP:        5000, // TODO
				MaxRequestsPerConn:   0,    // unlimited
				MaxKeepaliveDuration: options.HttpReadTimeout,
				ReadTimeout:          options.HttpReadTimeout,
				WriteTimeout:         options.HttpWriteTimeout,
				MaxRequestBodySize:   int(options.MaxPubSize + 1),
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
	waitListenerUp := make(chan struct{})
	go func() {
		if options.CpuAffinity {
			runtime.LockOSThread()
		}

		var (
			retryDelay  time.Duration
			theListener net.Listener
			addr        string
		)

		if https {
			this.httpsListener, err = net.Listen("tcp", this.httpsServer.Addr)
			this.httpsListener, _, err = setupHttpsListener(this.httpsListener, this.gw.certFile, this.gw.keyFile)
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

		theListener = FastListener(this.gw, theListener)
		close(waitListenerUp)

		for {
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
				// backoff
				if retryDelay == 0 {
					retryDelay = 5 * time.Millisecond
				} else {
					retryDelay = 2 * retryDelay
				}
				if maxDelay := time.Second; retryDelay > maxDelay {
					retryDelay = maxDelay
				}

				log.Error("%s server: %v, retry in %v", this.name, err, retryDelay)
				time.Sleep(retryDelay)
			}
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
		log.Info("%s https server ready on %s", this.name, this.httpsServer.Addr)
	} else {
		log.Info("%s http server ready on %s", this.name, this.httpServer.Addr)
	}

}

func (this *pubServer) waitExit(server *fastServerWithAddr, listener net.Listener, exit <-chan struct{}) {
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
}
