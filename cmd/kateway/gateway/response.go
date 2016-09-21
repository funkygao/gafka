package gateway

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

func punishClient() {
	time.Sleep(Options.BadClientPunishDuration)
}

func _writeErrorResponse(w http.ResponseWriter, err string, code int) {
	var out = map[string]string{
		"errmsg": err,
	}
	b, _ := json.Marshal(out)

	http.Error(w, string(b), code)
}

func writeNotFound(w http.ResponseWriter) {
	punishClient()

	w.Header().Set("Connection", "close")
	_writeErrorResponse(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func writeNotAllowed(w http.ResponseWriter) {
	punishClient()

	w.Header().Set("Connection", "close")
	_writeErrorResponse(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}

func writeAuthFailure(w http.ResponseWriter, err error) {
	punishClient()

	w.Header().Set("Connection", "close")
	_writeErrorResponse(w, err.Error(), http.StatusUnauthorized)
}

func writeWsError(ws *websocket.Conn, err string) {
	ws.WriteMessage(websocket.CloseMessage, []byte(err))
}

func writeQuotaExceeded(w http.ResponseWriter) {
	punishClient()
	punishClient() // twice on purpose

	w.Header().Set("Connection", "close")
	_writeErrorResponse(w, "quota exceeded", http.StatusTooManyRequests)
}

func writeServerError(w http.ResponseWriter, err string) {
	// internal server error, if client brutely retry without backoff, it will
	// hurt both server and client and its dependencies
	// so, kateway forcefully backoff client's retry interval
	time.Sleep(Options.InternalServerErrorBackoff)

	_writeErrorResponse(w, err, http.StatusInternalServerError)
}

func writeBadRequest(w http.ResponseWriter, err string) {
	punishClient()

	w.Header().Set("Connection", "close")
	_writeErrorResponse(w, err, http.StatusBadRequest)
}
