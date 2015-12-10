package main

import (
	"net"
	"sync"
)

// LimitListener returns a Listener that accepts at most n simultaneous
// connections from the provided Listener.
func LimitListener(gw *Gateway, l net.Listener, n int) net.Listener {
	return &limitListener{l, make(chan struct{}, n), gw}
}

type limitListener struct {
	net.Listener
	sem chan struct{}
	gw  *Gateway
}

func (l *limitListener) acquire() { l.sem <- struct{}{} }
func (l *limitListener) release() { <-l.sem }

func (l *limitListener) Accept() (net.Conn, error) {
	l.acquire()
	c, err := l.Listener.Accept()
	if l.gw != nil {
		l.gw.pubMetrics.ConnAccept.Inc(1)
		l.gw.pubMetrics.PubConcurrent.Inc(1)
	}

	if err != nil {
		l.release()
		return nil, err
	}
	return &limitListenerConn{Conn: c, release: l.release, gw: l.gw}, nil
}

type limitListenerConn struct {
	net.Conn
	releaseOnce sync.Once
	release     func()
	gw          *Gateway
}

func (l *limitListenerConn) Close() error {
	err := l.Conn.Close()
	if l.gw != nil {
		l.gw.pubMetrics.PubConcurrent.Dec(1)
	}
	l.releaseOnce.Do(l.release)
	return err
}
