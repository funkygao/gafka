package monitor

import (
	"encoding/json"
	"net/http"
	"time"
)

func writeServerError(w http.ResponseWriter, err string) {
	time.Sleep(time.Second) // todo, configurable

	_writeErrorResponse(w, err, http.StatusInternalServerError)
}

func _writeErrorResponse(w http.ResponseWriter, err string, code int) {
	var out = map[string]string{
		"errmsg": err,
	}
	b, _ := json.Marshal(out)

	http.Error(w, string(b), code)
}
