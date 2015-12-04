package main

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/funkygao/gafka/cmd/kateway/store"
)

func diff(l1, l2 []string) (added []string, deleted []string) {
	return
}

func writeKatewayHeader(w http.ResponseWriter) {
	w.Header().Set("Server", "kateway")
}

func writeAuthFailure(w http.ResponseWriter) {
	http.Error(w, "invalid secret", http.StatusUnauthorized)

	// close the suspicous http connection  TODO test case
	if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
		conn.Close()
	}
}

func writeBreakerOpen(w http.ResponseWriter) {
	http.Error(w, "circuit broken", http.StatusBadGateway)
}

func writeBadRequest(w http.ResponseWriter) {
	http.Error(w, "bad request", http.StatusBadRequest)
}

func isBrokerError(err error) bool {
	if err != store.ErrTooManyConsumers && err != store.ErrRebalancing {
		return true
	}

	return false
}

func getHttpQueryInt(r *http.Request, key string, defaultVal int) (int, error) {
	valStr := r.URL.Query().Get(key)
	if valStr == "" {
		return defaultVal, nil
	}

	return strconv.Atoi(valStr)
}

func getIp(r *http.Request) string {
	ip := r.Header.Get("X-Forward-For") // client_ip,proxy_ip,proxy_ip,...
	if ip == "" {
		return r.RemoteAddr
	}

	p := strings.SplitN(ip, ",", 1)
	return p[0]
}
