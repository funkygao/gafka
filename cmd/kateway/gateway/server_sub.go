package gateway

import (
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/golib/ratelimiter"
	log "github.com/funkygao/log4go"
)

type subServer struct {
	*webServer

	idleConnsWg   sync.WaitGroup      // wait for all inflight http connections done
	idleConns     map[string]net.Conn // in keep-alive state http connections
	closedConnCh  chan string         // channel of remote addr
	idleConnsLock sync.Mutex

	auditor log.Logger

	// websocket heartbeat configuration
	wsReadLimit int64
	wsPongWait  time.Duration

	shutdownOnce sync.Once
	ackShutdown  int32                                          // sync shutdown with ack handlers goroutines
	ackCh        chan ackOffsets                                // client ack'ed offsets
	ackedOffsets map[string]map[string]map[string]map[int]int64 // [cluster][topic][group][partition]: offset

	subMetrics        *subMetrics
	throttleSubStatus *ratelimiter.LeakyBuckets
}

func newSubServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *subServer {
	this := &subServer{
		webServer:         newWebServer("sub", httpAddr, httpsAddr, maxClients, gw),
		closedConnCh:      make(chan string, 1<<10),
		idleConns:         make(map[string]net.Conn, 10000), // TODO
		wsReadLimit:       8 << 10,
		wsPongWait:        time.Minute,
		throttleSubStatus: ratelimiter.NewLeakyBuckets(60, time.Minute),
		ackShutdown:       0,
		ackCh:             make(chan ackOffsets, 100),
		ackedOffsets:      make(map[string]map[string]map[string]map[int]int64),
	}
	this.subMetrics = NewSubMetrics(this.gw)
	this.waitExitFunc = this.waitExit
	this.connStateFunc = this.connStateHandler

	if this.httpsServer != nil {
		this.httpsServer.ConnState = this.connStateFunc
	}

	if this.httpServer != nil {
		this.httpServer.ConnState = this.connStateFunc
	}

	this.auditor = log.NewDefaultLogger(log.TRACE)
	this.auditor.DeleteFilter("stdout")

	rotateEnabled, discardWhenDiskFull := true, true
	filer := log.NewFileLogWriter("sub_audit.log", rotateEnabled, discardWhenDiskFull, 0644)
	filer.SetFormat("[%d %T] [%L] (%S) %M")
	if Options.LogRotateSize > 0 {
		filer.SetRotateSize(Options.LogRotateSize)
	}
	filer.SetRotateLines(0)
	filer.SetRotateDaily(true)
	this.auditor.AddFilter("file", logLevel, filer)

	return this
}

func (this *subServer) Start() {
	this.gw.wg.Add(1)
	go this.ackCommitter()

	this.subMetrics.Load()
	this.webServer.Start()
}

func (this *subServer) connStateHandler(c net.Conn, cs http.ConnState) {
	switch cs {
	case http.StateNew:
		// Connections begin at StateNew and then
		// transition to either StateActive or StateClosed
		this.idleConnsWg.Add(1)

		if this.gw != nil && !Options.DisableMetrics {
			this.gw.svrMetrics.ConcurrentSub.Inc(1)
		}

	case http.StateActive:
		// StateActive fires before the request has entered a handler
		// and doesn't fire again until the request has been
		// handled.
		// After the request is handled, the state
		// transitions to StateClosed, StateHijacked, or StateIdle.
		this.idleConnsLock.Lock()
		delete(this.idleConns, c.RemoteAddr().String())
		this.idleConnsLock.Unlock()

	case http.StateIdle:
		// StateIdle represents a connection that has finished
		// handling a request and is in the keep-alive state, waiting
		// for a new request. Connections transition from StateIdle
		// to either StateActive or StateClosed.
		select {
		case <-this.gw.shutdownCh:
			// actively close the client safely because IO is all done
			c.Close()

		default:
			this.idleConnsLock.Lock()
			this.idleConns[c.RemoteAddr().String()] = c
			this.idleConnsLock.Unlock()
		}

	case http.StateHijacked:
		// websocket steals the socket
		if this.gw != nil && !Options.DisableMetrics {
			this.gw.svrMetrics.ConcurrentSub.Dec(1)

			this.gw.svrMetrics.ConcurrentSubWs.Inc(1)
		}

	case http.StateClosed:
		if this.gw != nil && !Options.DisableMetrics {
			this.gw.svrMetrics.ConcurrentSub.Dec(1)
		}

		remoteAddr := c.RemoteAddr().String()
		if Options.EnableClientStats {
			this.gw.clientStates.UnregisterSubClient(remoteAddr)
		}

		this.closedConnCh <- remoteAddr
		this.idleConnsWg.Done()
	}
}

func (this *subServer) waitExit(server *http.Server, listener net.Listener, exit <-chan struct{}) {
	<-exit

	// HTTP response will have "Connection: close"
	server.SetKeepAlivesEnabled(false)

	// avoid new connections
	if err := listener.Close(); err != nil {
		log.Error(err.Error())
	}

	this.idleConnsLock.Lock()
	t := time.Now().Add(time.Millisecond * 100)
	for _, c := range this.idleConns {
		c.SetReadDeadline(t)
	}
	this.idleConnsLock.Unlock()

	log.Trace("%s waiting for all connected client close...", this.name)
	if waitTimeout(&this.idleConnsWg, Options.SubTimeout) {
		log.Warn("%s waiting for all connected client close timeout: %s",
			this.name, Options.SubTimeout)
	}

	this.subMetrics.Flush()

	this.gw.wg.Done()
}

func (this *subServer) ackCommitter() {
	ticker := time.NewTicker(time.Second * 30)
	defer func() {
		ticker.Stop()
		log.Debug("ack committer done")
		this.gw.wg.Done()
	}()

	for {
		select {
		case <-this.gw.shutdownCh:
			this.shutdownOnce.Do(func() {
				atomic.AddInt32(&this.ackShutdown, -1)

				for {
					// waiting for all ack handlers finish
					if atomic.LoadInt32(&this.ackShutdown) <= -1 {
						break
					}

					time.Sleep(time.Millisecond * 10)
				}
				close(this.ackCh)
			})

		case acks, ok := <-this.ackCh:
			if ok {
				for _, ack := range acks {
					if _, present := this.ackedOffsets[ack.cluster]; !present {
						this.ackedOffsets[ack.cluster] = make(map[string]map[string]map[int]int64)
					}
					if _, present := this.ackedOffsets[ack.cluster][ack.topic]; !present {
						this.ackedOffsets[ack.cluster][ack.topic] = make(map[string]map[int]int64)
					}
					if _, present := this.ackedOffsets[ack.topic][ack.group]; !present {
						this.ackedOffsets[ack.cluster][ack.topic][ack.group] = make(map[int]int64)
					}

					// TODO validation
					this.ackedOffsets[ack.cluster][ack.topic][ack.group][ack.Partition] = ack.Offset
				}
			} else {
				// channel buffer drained, flush all offsets
				// zk is still alive, safe to commit offsets
				this.commitOffsets()
				return
			}

		case <-ticker.C:
			this.commitOffsets()
		}
	}

}

func (this *subServer) commitOffsets() {
	for cluster, clusterTopic := range this.ackedOffsets {
		zkcluster := meta.Default.ZkCluster(cluster)

		for topic, groupPartition := range clusterTopic {
			for group, partitionOffset := range groupPartition {
				for partition, offset := range partitionOffset {
					if offset == -1 {
						// this slot is empty
						continue
					}

					log.Debug("commit offset {C:%s T:%s G:%s P:%d O:%d}", cluster, topic, group, partition, offset)

					if err := zkcluster.ResetConsumerGroupOffset(topic, group,
						strconv.Itoa(partition), offset); err != nil {
						log.Error("commitOffsets: %v", err)
					} else {
						// mark this slot empty
						this.ackedOffsets[cluster][topic][group][partition] = -1
					}
				}
			}
		}
	}

}
