package main

import (
	"crypto/tls"
	"net"
)

func setupHttpsListener(listenAddr string, certFile, keyFile string) (net.Listener, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	cer, err := tls.LoadX509KeyPair("server.pem", "server.key")
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
