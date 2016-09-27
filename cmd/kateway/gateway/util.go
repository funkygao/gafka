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

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/zk"
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
		// directly connected
		idx := strings.LastIndex(r.RemoteAddr, ":")
		if idx == -1 {
			return r.RemoteAddr
		}

		return r.RemoteAddr[:idx]
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

type SubStatus struct {
	Appid          string `json:"appid,omitempty"`
	Group          string `json:"group"`
	Partition      string `json:"partition"`
	ProducedOldest int64  `json:"pold"`
	ProducedNewest int64  `json:"pubd"`
	Consumed       int64  `json:"subd"`
	ClientRealIP   string `json:"realip"`
}

func topicSubStatus(cluster string, myAppid, hisAppid, topic, ver string,
	group string, onlyMine bool) ([]SubStatus, error) {
	zkcluster := meta.Default.ZkCluster(cluster)
	if group != "" {
		// if group is empty, find all groups
		group = myAppid + "." + group
	}
	rawTopic := manager.Default.KafkaTopic(hisAppid, topic, ver)
	consumersByGroup, err := zkcluster.ConsumerGroupsOfTopic(rawTopic)
	if err != nil {
		return nil, err
	}
	sortedGroups := make([]string, 0, len(consumersByGroup))
	for grp := range consumersByGroup {
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
			if len(p) != 2 || (onlyMine && myAppid != p[0]) {
				// this group does not belong to me
				continue
			}

			var realIP string
			if consumer.Online && consumer.ConsumerZnode != nil {
				realIP = consumer.ConsumerZnode.ClientRealIP()
			}

			stat := SubStatus{
				Group:          p[1],
				Partition:      consumer.PartitionId,
				ProducedOldest: consumer.OldestOffset,
				ProducedNewest: consumer.ProducerOffset,
				Consumed:       consumer.ConsumerOffset,
				ClientRealIP:   realIP,
			}
			if !onlyMine {
				stat.Appid = p[0]
			}

			out = append(out, stat)
		}
	}

	return out, nil
}
