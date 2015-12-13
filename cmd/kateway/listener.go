package main

import (
	"net"
	"sync"
)

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
	err := c.Conn.Close()
	if c.gw != nil && !options.disableMetrics {
		c.gw.pubMetrics.PubConcurrent.Dec(1)
	}
	c.releaseOnce.Do(c.release)
	return err
}
