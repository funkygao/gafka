package main

import (
	"crypto/tls"
	"net"
	"net/http"
)

type waitExitFunc func(server *http.Server, listener net.Listener, exit <-chan struct{})
type connStateFunc func(c net.Conn, cs http.ConnState)
type onConnNewFunc func(net.Conn)
type onConnCloseFunc func(net.Conn)

func setupHttpsListener(listener net.Listener, certFile, keyFile string) (net.Listener, error) {
	cer, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	config := &tls.Config{
		NextProtos:   []string{"http/1.1"},
		Certificates: []tls.Certificate{cer},
	}

	tlsListener := tls.NewListener(listener, config)
	return tlsListener, nil
}
