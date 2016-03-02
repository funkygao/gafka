package main

import (
	"encoding/json"
	"net/http"
)

func (this *Gateway) writeErrorResponse(w http.ResponseWriter, err string, code int) {
	var out = map[string]string{
		"errmsg": err,
	}
	b, _ := json.Marshal(out)

	w.Header().Set(ContentTypeHeader, ContentTypeJson)
	this.writeKatewayHeader(w)
	http.Error(w, string(b), code)
}

func (this *Gateway) writeInvalidContentLength(w http.ResponseWriter) {
	this.writeErrorResponse(w, "invalid content length", http.StatusBadRequest)
}

func (this *Gateway) writeKatewayHeader(w http.ResponseWriter) {
	w.Header().Set("Server", "kateway")
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

func (this *Gateway) writeBadRequest(w http.ResponseWriter, err error) {
	this.writeErrorResponse(w, err.Error(), http.StatusBadRequest)
}
