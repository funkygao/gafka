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

	// X-Forwarded-For: 10.1.1.1, 10.1.1.2
	// use 10.1.1.1(head) as real ip: middleman will append instead of insert
	if commaIdx := strings.Index(forwardFor, ","); commaIdx == -1 {
		// not nested
		return forwardFor
	} else {
		// head in the chain
		return forwardFor[:commaIdx]
	}
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
	Topic          string `json:"topic,omitempty"`
	Partition      string `json:"partition"`
	ProducedOldest int64  `json:"pold"`
	ProducedNewest int64  `json:"pubd"`
	Consumed       int64  `json:"subd"`
	ClientRealIP   string `json:"realip"`
}

func topicSubStatus(cluster string, myAppid, hisAppid, topic, ver string,
	group string, onlyMine bool) ([]SubStatus, error) {
	zkcluster := meta.Default.ZkCluster(cluster)

	if hisAppid == "" && topic == "" && ver == "" {
		// app sub status: only display online consumers
		out := make([]SubStatus, 0, 10)

		consumersByGroup := zkcluster.ConsumersByGroup(myAppid + ".")
		sortedGroups := make([]string, 0, len(consumersByGroup))
		for group := range consumersByGroup {
			sortedGroups = append(sortedGroups, group)
		}
		sort.Strings(sortedGroups)

		for _, group := range sortedGroups {
			p := strings.SplitN(group, ".", 2)
			if len(p) != 2 {
				continue
			}

			sortedTopicAndPartitionIds := make([]string, 0, len(consumersByGroup[group]))
			consumers := make(map[string]zk.ConsumerMeta)
			for _, t := range consumersByGroup[group] {
				key := fmt.Sprintf("%s:%s", t.Topic, t.PartitionId)
				sortedTopicAndPartitionIds = append(sortedTopicAndPartitionIds, key)

				consumers[key] = t
			}
			sort.Strings(sortedTopicAndPartitionIds)

			for _, topicAndPartitionId := range sortedTopicAndPartitionIds {
				consumer := consumers[topicAndPartitionId]
				parts := strings.SplitN(topicAndPartitionId, ":", 2)
				if len(parts) != 2 {
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
					Appid:          myAppid,
					Topic:          parts[0],
				}
				out = append(out, stat)
			}
		}

		return out, nil
	}

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
