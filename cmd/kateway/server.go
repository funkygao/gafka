package main

import (
	"net"
	"net/http"
	"time"
)

var (
	httpReadTimeout    = time.Minute * 5 // TODO
	httpWriteTimeout   = time.Minute
	httpHeaderMaxBytes = 4 << 10
)

type waitExitFunc func(server *http.Server, listener net.Listener, exit <-chan struct{})
