package main

import (
	"code.google.com/p/go.net/websocket"
)

func (this *Gateway) pubWsHandler(ws *websocket.Conn) {
	defer ws.Close()
}
