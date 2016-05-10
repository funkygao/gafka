package gateway

import (
	"net"
	"sync"

	log "github.com/funkygao/log4go"
)

type fastListener struct {
	net.Listener
	gw *Gateway
}

func FastListener(gw *Gateway, l net.Listener) net.Listener {
	return &fastListener{
		Listener: l,
		gw:       gw,
	}
}

func (l *fastListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	log.Debug("%s new conn from %s", l.Listener.Addr(), c.RemoteAddr())
	if l.gw != nil && !Options.DisableMetrics {
		l.gw.svrMetrics.TotalConns.Inc(1)
		l.gw.svrMetrics.ConcurrentConns.Inc(1)
	}

	return &fastListenerConn{c, l.gw}, nil
}

type fastListenerConn struct {
	net.Conn
	gw *Gateway
}

func (c *fastListenerConn) Close() error {
	log.Debug("%s closing conn from %s", c.Conn.LocalAddr(), c.Conn.RemoteAddr())

	err := c.Conn.Close()
	if c.gw != nil && !Options.DisableMetrics {
		c.gw.svrMetrics.ConcurrentConns.Dec(1)
	}
	return err
}

type limitListener struct {
	net.Listener
	throttler chan struct{}
	gw        *Gateway
	name      string
}

// LimitListener returns a Listener that accepts at most n simultaneous
// connections from the provided Listener.
func LimitListener(name string, gw *Gateway, l net.Listener, n int) net.Listener {
	return &limitListener{
		Listener:  l,
		throttler: make(chan struct{}, n),
		gw:        gw,
		name:      name}
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

	log.Debug("%s[%s] new conn from %s", l.name, l.Listener.Addr(), c.RemoteAddr())
	if l.gw != nil && !Options.DisableMetrics {
		l.gw.svrMetrics.TotalConns.Inc(1)
		l.gw.svrMetrics.ConcurrentConns.Inc(1)
	}

	return &limitListenerConn{
		Conn:    c,
		release: l.release,
		gw:      l.gw,
		name:    l.name}, nil
}

type limitListenerConn struct {
	net.Conn
	releaseOnce sync.Once
	release     func()
	gw          *Gateway
	name        string
}

func (c *limitListenerConn) Close() error {
	log.Debug("%s[%s] closing conn from %s", c.name, c.Conn.LocalAddr(), c.Conn.RemoteAddr())

	err := c.Conn.Close()
	if c.gw != nil && !Options.DisableMetrics {
		c.gw.svrMetrics.ConcurrentConns.Dec(1)
	}
	c.releaseOnce.Do(c.release)
	return err
}
