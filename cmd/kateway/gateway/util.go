package gateway

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
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

// getHttpRemoteIp returns ip only, without remote port.
func getHttpRemoteIp(r *http.Request) string {
	forwardFor := r.Header.Get(HttpHeaderXForwardedFor) // client_ip,proxy_ip,proxy_ip,...
	if forwardFor == "" {
		// not behind haproxy, directly connected
		p := strings.SplitN(r.RemoteAddr, ":", 2)
		return p[0]
	}

	return forwardFor // FIXME forwardFor might be comma seperated ip list, but here for performance ignore it
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

type SubStatus struct {
	Appid          string `json:"appid,omitempty"`
	Group          string `json:"group"`
	Partition      string `json:"partition"`
	ProducedOldest int64  `json:"pold"`
	ProducedNewest int64  `json:"pubd"`
	Consumed       int64  `json:"subd"`
}

func topicSubStatus(cluster string, myAppid, hisAppid, topic, ver string,
	group string, onlyMine bool) ([]SubStatus, error) {
	zkcluster := meta.Default.ZkCluster(cluster)
	if group != "" {
		// if group is empty, find all groups
		group = myAppid + "." + group
	}
	rawTopic := manager.KafkaTopic(hisAppid, topic, ver)
	consumersByGroup, err := zkcluster.ConsumerGroupsOfTopic(rawTopic)
	if err != nil {
		return nil, err
	}
	sortedGroups := make([]string, 0, len(consumersByGroup))
	for grp, _ := range consumersByGroup {
		sortedGroups = append(sortedGroups, grp)
	}
	sort.Strings(sortedGroups)

	out := make([]SubStatus, 0, len(sortedGroups))
	for _, grp := range sortedGroups {
		if group != "" && grp != group {
			continue
		}

		sortedTopicAndPartitionIds := make([]string, 0, len(consumersByGroup[grp]))
		consumers := make(map[string]zk.ConsumerMeta)
		for _, t := range consumersByGroup[grp] {
			key := fmt.Sprintf("%s:%s", t.Topic, t.PartitionId)
			sortedTopicAndPartitionIds = append(sortedTopicAndPartitionIds, key)

			consumers[key] = t
		}
		sort.Strings(sortedTopicAndPartitionIds)

		for _, topicAndPartitionId := range sortedTopicAndPartitionIds {
			consumer := consumers[topicAndPartitionId]
			if rawTopic != consumer.Topic {
				continue
			}

			p := strings.SplitN(grp, ".", 2) // grp is like 'appid.groupname'
			if onlyMine && myAppid != p[0] {
				// this group does not belong to me
				continue
			}

			stat := SubStatus{
				Group:          p[1],
				Partition:      consumer.PartitionId,
				ProducedOldest: consumer.OldestOffset,
				ProducedNewest: consumer.ProducerOffset,
				Consumed:       consumer.ConsumerOffset,
			}
			if !onlyMine {
				stat.Appid = p[0]
			}

			out = append(out, stat)
		}
	}

	return out, nil
}
