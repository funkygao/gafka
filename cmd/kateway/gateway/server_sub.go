package gateway

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/golib/ratelimiter"
	"github.com/funkygao/golib/sync2"
	log "github.com/funkygao/log4go"
)

type subServer struct {
	*webServer

	idleConnsWg   sync2.WaitGroupTimeout // wait for all inflight http connections done
	closedConnCh  chan string            // channel of remote addr
	idleConns     map[net.Conn]struct{}
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
		idleConns:         make(map[net.Conn]struct{}, 200),
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

	_ = os.Mkdir("audit", os.ModePerm)
	rotateEnabled, discardWhenDiskFull := true, false
	filer := log.NewFileLogWriter("audit/sub_audit.log", rotateEnabled, discardWhenDiskFull, 0644)
	if filer == nil {
		panic("failed to open sub audit log")
	}
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
		atomic.AddInt32(&this.activeConnN, 1)

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
		delete(this.idleConns, c)
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

			this.idleConnsLock.Lock()
			delete(this.idleConns, c)
			this.idleConnsLock.Unlock()

		default:
			this.idleConnsLock.Lock()
			this.idleConns[c] = struct{}{}
			this.idleConnsLock.Unlock()
		}

	case http.StateHijacked:
		// websocket steals the socket
		this.idleConnsLock.Lock()
		delete(this.idleConns, c)
		this.idleConnsLock.Unlock()

		atomic.AddInt32(&this.activeConnN, -1)

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
		atomic.AddInt32(&this.activeConnN, -1)

		this.idleConnsLock.Lock()
		delete(this.idleConns, c)
		this.idleConnsLock.Unlock()
	}
}

func (this *subServer) waitExit(exit <-chan struct{}) {
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

	this.idleConnsLock.Lock()
	t := time.Now().Add(time.Millisecond * 100)
	for c := range this.idleConns {
		c.SetReadDeadline(t)
	}
	this.idleConnsLock.Unlock()

	if this.idleConnsWg.WaitTimeout(Options.SubTimeout) {
		log.Warn("%s waiting for all connected client close timeout: %s",
			this.name, Options.SubTimeout)
	}

	this.subMetrics.Flush()

	this.gw.wg.Done()
	close(this.closed)
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
