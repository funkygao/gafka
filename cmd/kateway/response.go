package main

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/websocket"
)

func (this *Gateway) writeErrorResponse(w http.ResponseWriter, err string, code int) {
	var out = map[string]string{
		"errmsg": err,
	}
	b, _ := json.Marshal(out)

	http.Error(w, string(b), code)
}

func (this *Gateway) writeAuthFailure(w http.ResponseWriter, err error) {
	// close the suspicous http connection
	w.Header().Set("Connection", "close")

	this.writeErrorResponse(w, err.Error(), http.StatusUnauthorized)
}

func (this *Gateway) writeWsError(ws *websocket.Conn, err string) {
	ws.WriteMessage(websocket.CloseMessage, []byte(err))
}

func (this *Gateway) writeQuotaExceeded(w http.ResponseWriter) {
	w.Header().Set("Connection", "close")

	this.writeErrorResponse(w, "quota exceeded", http.StatusNotAcceptable)
}

func (this *Gateway) writeServerError(w http.ResponseWriter, err string) {
	this.writeErrorResponse(w, err, http.StatusInternalServerError)
}

func (this *Gateway) writeBadRequest(w http.ResponseWriter, err string) {
	w.Header().Set("Connection", "close")

	this.writeErrorResponse(w, err, http.StatusBadRequest)
}
