package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/zk"
)

type SubStatus struct {
	Appid     string `json:"appid"`
	Group     string `json:"group"`
	Partition string `json:"partition"`
	Produced  int64  `json:"pubd"`
	Consumed  int64  `json:"subd"`
}

func (this *Gateway) topicSubStatus(cluster string, myAppid, hisAppid, topic, ver string,
	group string, onlyMine bool) ([]SubStatus, error) {
	zkcluster := meta.Default.ZkCluster(cluster)
	if group != "" {
		group = myAppid + "." + group
	}
	rawTopic := meta.KafkaTopic(hisAppid, topic, ver)
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

			out = append(out, SubStatus{
				Appid:     p[0],
				Group:     p[1],
				Partition: consumer.PartitionId,
				Produced:  consumer.ProducerOffset,
				Consumed:  consumer.ConsumerOffset,
			})
		}
	}

	return out, nil
}
