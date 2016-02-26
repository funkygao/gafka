package main

import (
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/go-metrics"
)

var (
	topicNameRegex = regexp.MustCompile(`[a-zA-Z0-9\-_]+`)
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

func getHttpRemoteIp(r *http.Request) string {
	ip := r.Header.Get(HttpHeaderXForwardedFor) // client_ip,proxy_ip,proxy_ip,...
	if ip == "" {
		return r.RemoteAddr // ip:port
	}

	p := strings.SplitN(ip, ",", 1)
	return p[0]
}

func validateTopicName(topic string) bool {
	return len(topicNameRegex.FindAllString(topic, -1)) == 1
}

func checkUlimit(min int) {
	ulimitN, err := exec.Command("/bin/sh", "-c", "ulimit -n").Output()
	if err != nil {
		panic(err)
	}

	n, err := strconv.Atoi(strings.TrimSpace(string(ulimitN)))
	if err != nil || n < min {
		log.Panicf("ulimit too small: %d, should be at least %d", n, min)
	}
}

func extractFromMetricsName(name string) (appid, topic, ver, realname string) {
	if name[0] != '{' {
		realname = name
		return
	}

	i := strings.Index(name, "}")
	realname = name[i+1:]
	p := strings.SplitN(name[1:i], ".", 3)
	appid, topic, ver = p[0], p[1], p[2]
	return
}

func updateCounter(appid, topic, ver, name string, n int64,
	mu *sync.RWMutex, m map[string]metrics.Counter) {
	tagBuf := make([]byte, 4+len(appid)+len(topic)+len(ver))
	tagBuf[0] = CharBraceletLeft
	idx := 1
	for ; idx <= len(appid); idx++ {
		tagBuf[idx] = appid[idx-1]
	}
	tagBuf[idx] = CharDot
	idx++
	for j := 0; j < len(topic); j++ {
		tagBuf[idx+j] = topic[j]
	}
	idx += len(topic)
	tagBuf[idx] = CharDot
	idx++
	for j := 0; j < len(ver); j++ {
		tagBuf[idx+j] = ver[j]
	}
	idx += len(ver)
	tagBuf[idx] = CharBraceletRight

	mu.RLock()
	// golang has optimization avoids extra allocations when []byte keys are used to
	// lookup entries in map[string] collections: m[string(key)]
	counter, present := m[string(tagBuf)]
	mu.RUnlock()

	if present {
		counter.Inc(1)
		return
	}

	// seldom goes here, needn't optimize

	tag := string(tagBuf)
	mu.Lock()
	m[tag] = metrics.NewRegisteredCounter(tag+name, metrics.DefaultRegistry)
	mu.Unlock()

	m[tag].Inc(n)
}
