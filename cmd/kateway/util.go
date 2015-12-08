package main

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/funkygao/gafka/cmd/kateway/store"
)

func isBrokerError(err error) bool {
	if err != store.ErrTooManyConsumers && err != store.ErrRebalancing {
		return true
	}

	return false
}

func getHttpQueryInt(query *url.Values, key string, defaultVal int) (int, error) {
	valStr := query.Get(key)
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
