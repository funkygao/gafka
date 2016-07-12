package telementry

import (
	"strings"
	"sync"

	"github.com/funkygao/go-metrics"
)

const (
	charBraceletLeft  = '{'
	charBraceletRight = '}'
	charDot           = '.'
)

// Untag will extract the tags info from [encoded] metric name: sanitizeName
//
// go-metrics pkg doesn't support tags feature, so we encode the tags
// into metric name.
func Untag(name string) (appid, topic, ver, realname string) {
	if name[0] != charBraceletLeft {
		// the name is not tagged
		realname = name
		return
	}

	i := strings.IndexByte(name, charBraceletRight)
	realname = name[i+1:]
	p := strings.SplitN(name[1:i], ".", 3)
	appid, topic, ver = p[0], p[1], p[2]
	return
}

func Tag(appid, topic, ver string) string {
	tagBuf := make([]byte, 4+len(appid)+len(topic)+len(ver))
	tagBuf[0] = charBraceletLeft
	idx := 1
	for ; idx <= len(appid); idx++ {
		tagBuf[idx] = appid[idx-1]
	}
	tagBuf[idx] = charDot
	idx++
	for j := 0; j < len(topic); j++ {
		tagBuf[idx+j] = topic[j]
	}
	idx += len(topic)
	tagBuf[idx] = charDot
	idx++
	for j := 0; j < len(ver); j++ {
		tagBuf[idx+j] = ver[j]
	}
	idx += len(ver)
	tagBuf[idx] = charBraceletRight

	return string(tagBuf)
}

func UpdateCounter(appid, topic, ver, name string, n int64,
	mu *sync.RWMutex, m map[string]metrics.Counter) {
	// Fast path
	tagBuf := make([]byte, 4+len(appid)+len(topic)+len(ver))
	tagBuf[0] = charBraceletLeft
	idx := 1
	for ; idx <= len(appid); idx++ {
		tagBuf[idx] = appid[idx-1]
	}
	tagBuf[idx] = charDot
	idx++
	for j := 0; j < len(topic); j++ {
		tagBuf[idx+j] = topic[j]
	}
	idx += len(topic)
	tagBuf[idx] = charDot
	idx++
	for j := 0; j < len(ver); j++ {
		tagBuf[idx+j] = ver[j]
	}
	idx += len(ver)
	tagBuf[idx] = charBraceletRight

	mu.RLock()
	// golang has optimization avoids extra allocations when []byte keys are used to
	// lookup entries in map[string] collections: m[string(key)]
	counter, present := m[string(tagBuf)]
	mu.RUnlock()

	if present {
		counter.Inc(n)
		return
	}

	// seldom goes here, needn't optimize

	tag := string(tagBuf)
	mu.Lock()
	m[tag] = metrics.NewRegisteredCounter(tag+name, nil)
	mu.Unlock()

	m[tag].Inc(n)
}
