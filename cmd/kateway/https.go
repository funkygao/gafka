package main

import (
	"crypto/tls"
	"net"
)

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
