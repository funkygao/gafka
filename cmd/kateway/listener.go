package main

import (
	"net"
	"sync"

	log "github.com/funkygao/log4go"
)

type fastListener struct {
	net.Listener
	gw *Gateway
}

func FastListener(l net.Listener, gw *Gateway) net.Listener {
	return &fastListener{l, gw}
}

func (l *fastListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	log.Trace("%s got conn %s", l.Listener.Addr(), c.RemoteAddr())
	if l.gw != nil && !options.disableMetrics {
		l.gw.pubMetrics.ConnAccept.Inc(1)
		l.gw.pubMetrics.PubConcurrent.Inc(1)
	}

	return &fastListenerConn{c, l.gw}, nil
}

type fastListenerConn struct {
	net.Conn
	gw *Gateway
}

func (c *fastListenerConn) Close() error {
	log.Trace("%s closing conn %s", c.Conn.LocalAddr(), c.Conn.RemoteAddr())

	err := c.Conn.Close()
	if c.gw != nil && !options.disableMetrics {
		c.gw.pubMetrics.PubConcurrent.Dec(1)
	}
	return err
}

type limitListener struct {
	net.Listener
	throttler chan struct{}
	gw        *Gateway
}

// LimitListener returns a Listener that accepts at most n simultaneous
// connections from the provided Listener.
func LimitListener(gw *Gateway, l net.Listener, n int) net.Listener {
	return &limitListener{l, make(chan struct{}, n), gw}
}

func (l *limitListener) acquire() { l.throttler <- struct{}{} }
func (l *limitListener) release() { <-l.throttler }

func (l *limitListener) Accept() (net.Conn, error) {
	l.acquire()
	c, err := l.Listener.Accept()
	if err != nil {
		l.release()
		return nil, err
	}

	log.Trace("%s got conn %s", l.Listener.Addr(), c.RemoteAddr())
	if l.gw != nil && !options.disableMetrics {
		l.gw.pubMetrics.ConnAccept.Inc(1)
		l.gw.pubMetrics.PubConcurrent.Inc(1)
	}

	return &limitListenerConn{Conn: c, release: l.release, gw: l.gw}, nil
}

type limitListenerConn struct {
	net.Conn
	releaseOnce sync.Once
	release     func()
	gw          *Gateway
}

func (c *limitListenerConn) Close() error {
	log.Trace("%s closing conn %s", c.Conn.LocalAddr(), c.Conn.RemoteAddr())

	err := c.Conn.Close()
	if c.gw != nil && !options.disableMetrics {
		c.gw.pubMetrics.PubConcurrent.Dec(1)
	}
	c.releaseOnce.Do(c.release)
	return err
}
