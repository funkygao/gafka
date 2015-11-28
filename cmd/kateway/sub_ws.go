package main

import (
	"code.google.com/p/go.net/websocket"
)

func (this *Gateway) subWsHandler(ws *websocket.Conn) {
	defer ws.Close()
}
