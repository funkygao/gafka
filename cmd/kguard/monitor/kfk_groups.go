package monitor

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type GroupMember struct {
	PartitionId int32  `json:"partition_id"`
	MemId       string `json:"member_id"`
	UpTimeSec   int64  `json:"uptime_sec"`
}

type GroupOffset struct {
	Offset    int64 `json:"offset"`
	CommitSec int64 `json:"commit_sec"`
}

type MemberOrderById []*GroupMember

func (this MemberOrderById) Len() int           { return len(this) }
func (this MemberOrderById) Less(i, j int) bool { return this[i].PartitionId < this[j].PartitionId }
func (this MemberOrderById) Swap(i, j int)      { this[i], this[j] = this[j], this[i] }

type Group struct {
	Name    string                 `json:"group"`
	Topic   string                 `json:"topic"`
	Offsets map[int32]*GroupOffset `json:"offsets"`
	Online  bool                   `json:"online"`
	Members []*GroupMember         `json:"members"`
	ErrMsg  string                 `json:"error"`
}

type PartitionOffset struct {
	Id     int32 `json:"id"`
	Oldest int64 `json:"oldest"`
	Latest int64 `json:"latest"`
}

type PartitionOffsetOrderById []*PartitionOffset

func (this PartitionOffsetOrderById) Len() int           { return len(this) }
func (this PartitionOffsetOrderById) Less(i, j int) bool { return this[i].Id < this[j].Id }
func (this PartitionOffsetOrderById) Swap(i, j int)      { this[i], this[j] = this[j], this[i] }

type TopicGroups struct {
	Topic            string             `json:"topic"`
	PartitionOffsets []*PartitionOffset `json:"partition_offsets"`
	Groups           []*Group           `json:"groups"`
	ErrMsg           string             `json:"error"`
}

type ClusterTopicsGroups struct {
	Name        string         `json:"cluster_name"`
	TopicGroups []*TopicGroups `json:"topics_groups"`
	ErrMsg      string         `json:"error"`
}

type AllTopicsGroups struct {
	All []*ClusterTopicsGroups `json:"all_topics_groups"`
}

// find relationship between topics and groups within one cluster
// return:
//     allTopics map[string]time.Time, all topics map
//         topic-->time
// 	   allGroups map[string]map[string]*gzk.ConsumerZnode, each group has how many member and who are they, with sub info
//         group-->memId-->node
//     topicsGroups map[string]map[string]map[string]int64, each topic has how many groups and who are they
//         topic-->group-->partitionId-->offset
//     groupsTopics map[string]map[string]map[string]int64, each group sub how many topics and who are they
//         group-->topic-->partitionId-->offset
func (this *Monitor) getClusterTopicsGroupsInfo(cluster string) (allTopics map[string]time.Time,
	allGroups map[string]map[string]*gzk.ConsumerZnode,
	topicsGroupsOffset map[string]map[string]map[string]int64,
	groupsTopicsOffset map[string]map[string]map[string]int64,
	err error) {

	zkCluster := this.zkzone.NewCluster(cluster)

	topicsGroupsOffset = make(map[string]map[string]map[string]int64)
	groupsTopicsOffset = make(map[string]map[string]map[string]int64)
	allTopics = zkCluster.TopicsCtime()
	allGroups = zkCluster.ConsumerGroups()

	for g, _ := range allGroups {
		groupOffsets := zkCluster.ConsumerOffsetsOfGroup(g) // sub this topic: has offset on this topic

		for t, to := range groupOffsets {
			var topicGroups map[string]map[string]int64
			var groupTopics map[string]map[string]int64
			var present = false

			if topicGroups, present = topicsGroupsOffset[t]; !present {
				topicGroups = make(map[string]map[string]int64)
				topicsGroupsOffset[t] = topicGroups
			}

			if groupTopics, present = groupsTopicsOffset[g]; !present {
				groupTopics = make(map[string]map[string]int64)
				groupsTopicsOffset[g] = groupTopics
			}

			topicGroups[g] = to
			groupTopics[t] = to
		}
	}

	return
}

// @rest POST /kfk/topicsGroups
// find group's info under specific topic (cluster-->topics-->groups)
// 1. specify cluster or all clusters, return error msg if cluster not exist
// 2. specify topic or all topics, return error msg if topic not exist
// 3. specify group or all groups, return error if group not on target topic or group not exist
// 4. return topics oldest and latest offset for each partition
// 5. return group's offset on topic for each partition and last commit time
// 6. return online group memId and up time
// eg: curl -XPOST http://192.168.149.150:10025/kfk/topicsGroups -d'[{"cluster":"bigtopic_cluster_02", "topics_groups":[{"topic":"cluster_02_topic_01", "groups":["my_cluster_02_cg_01"]}]}]'
func (this *Monitor) kfkTopicsGroupsHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {

	defer func() {
		if err := recover(); err != nil {
			log.Error("%+v", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}()

	type topicsGroupsRequestItem struct {
		Topic  string   `json:"topic"`
		Groups []string `json:"groups"`
	}

	type clustersTopicsGroupsRequestItem struct {
		Cluster      string                    `json:"cluster"`
		TopicsGroups []topicsGroupsRequestItem `json:"topics_groups"`
	}

	type clustersTopicsGroupsRequest []clustersTopicsGroupsRequestItem

	dec := json.NewDecoder(r.Body)
	var req clustersTopicsGroupsRequest
	err := dec.Decode(&req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	reqInfo := make(map[string]map[string][]string) // cluster --> topic --> groups
	for _, c := range req {
		topicsgroups := make(map[string][]string)

		for _, tg := range c.TopicsGroups {
			topicsgroups[tg.Topic] = tg.Groups
		}

		reqInfo[c.Cluster] = topicsgroups
	}

	topicsGroups, err := this.getTopicsGroups(reqInfo)
	if err != nil {
		log.Error("topics_groups:%v %v", reqInfo, err)
		writeServerError(w, err.Error())
		return
	}

	b, _ := json.Marshal(topicsGroups)
	w.Write(b)

	return
}

func (this *Monitor) getTopicsGroups(reqClusters map[string]map[string][]string) (allTopicsGroups AllTopicsGroups, err error) {

	allClusters := this.zkzone.Clusters()

	// preallocate all slot
	if len(reqClusters) == 0 {
		// we need to get all clusters
		for cn, _ := range allClusters {
			ctg := ClusterTopicsGroups{
				Name: cn,
			}

			allTopicsGroups.All = append(allTopicsGroups.All, &ctg)
		}
	} else {
		// we need to get target clusters
		for reqC, _ := range reqClusters {
			ctg := ClusterTopicsGroups{
				Name: reqC,
			}
			// check cluster exist or not
			if _, ok := allClusters[reqC]; !ok {
				ctg.ErrMsg = ErrClusterNotExist.Error()
			}

			allTopicsGroups.All = append(allTopicsGroups.All, &ctg)
		}
	}

	for _, ctg := range allTopicsGroups.All {
		if ctg.ErrMsg == "" { // valid cluster
			reqTopics, _ := reqClusters[ctg.Name]
			err = this.getClusterTopicsGroups(ctg, reqTopics)
			if err != nil {
				return
			}
		}
	}

	return
}

func (this *Monitor) getClusterTopicsGroups(ctg *ClusterTopicsGroups, reqTopics map[string][]string) (err error) {

	// cluster topic/group relationship summary info
	// once get these, we can use it on each group info's query procedure
	allTopics, allGroups, topicsGroupsOffset, _, err := this.getClusterTopicsGroupsInfo(ctg.Name)
	if err != nil {
		return
	}

	zkCluster := this.zkzone.NewCluster(ctg.Name)
	kfkClient, err := sarama.NewClient(zkCluster.BrokerList(), saramaConfig())
	if err != nil {
		return
	}
	defer kfkClient.Close()

	if len(reqTopics) == 0 {
		// all topics info
		for tn, _ := range allTopics {
			tg := TopicGroups{
				Topic: tn,
			}
			ctg.TopicGroups = append(ctg.TopicGroups, &tg)
		}
	} else {
		// we need to get target topics
		for reqT, _ := range reqTopics {
			tgs := TopicGroups{
				Topic: reqT,
			}
			// check topic exist or not
			if _, ok := allTopics[reqT]; !ok {
				tgs.ErrMsg = ErrTopicNotExist.Error()
			}

			ctg.TopicGroups = append(ctg.TopicGroups, &tgs)
		}
	}

	for _, tgs := range ctg.TopicGroups {
		if tgs.ErrMsg == "" { // valid cluster
			reqGroups, _ := reqTopics[tgs.Topic]
			err = this.getTopicGroups(zkCluster, kfkClient, allGroups, topicsGroupsOffset, tgs, reqGroups)
			if err != nil {
				return
			}
		}
	}

	return
}

func (this *Monitor) getTopicGroups(zkCluster *gzk.ZkCluster,
	kfkClient sarama.Client,
	allGroups map[string]map[string]*gzk.ConsumerZnode,
	topicsGroupsOffset map[string]map[string]map[string]int64,
	tgs *TopicGroups, reqGroups []string) (err error) {

	allGroupsOnTopic := topicsGroupsOffset[tgs.Topic]
	if len(reqGroups) == 0 {
		// all group on this topic
		for gn, _ := range allGroupsOnTopic {
			g := Group{
				Name:    gn,
				Topic:   tgs.Topic,
				Offsets: make(map[int32]*GroupOffset),
			}
			tgs.Groups = append(tgs.Groups, &g)
		}
	} else {
		// we need to get target groups
		for _, reqG := range reqGroups {
			g := Group{
				Name:    reqG,
				Topic:   tgs.Topic,
				Offsets: make(map[int32]*GroupOffset),
			}
			// check group exist or not
			if allGroups == nil {
				g.ErrMsg = ErrGroupNotExist.Error()
			} else if _, ok := allGroups[reqG]; !ok {
				g.ErrMsg = ErrGroupNotExist.Error()
			} else if allGroupsOnTopic == nil {
				g.ErrMsg = ErrGroupNotOnTopic.Error()
			} else if _, ok := allGroupsOnTopic[reqG]; !ok {
				g.ErrMsg = ErrGroupNotOnTopic.Error()
			}

			tgs.Groups = append(tgs.Groups, &g)
		}
	}

	var null struct{}
	existedGroups := make(map[string]struct{})
	for g, _ := range allGroupsOnTopic {
		existedGroups[g] = null
	}

	for _, g := range tgs.Groups {
		if g.ErrMsg == "" { // valid cluster
			err = this.getGroup(zkCluster, allGroups, existedGroups, g)
			if err != nil {
				return
			}
		}
	}

	// after get group info , get topic's oldest and latest
	allPartitions, err := kfkClient.Partitions(tgs.Topic)
	if err != nil {
		return
	}

	for _, pid := range allPartitions {
		var oldest, latest int64
		oldest, latest, err = this.getPartitionOffset(kfkClient, tgs.Topic, pid)
		if err != nil {
			return
		}

		po := PartitionOffset{
			Oldest: oldest,
			Latest: latest,
		}

		tgs.PartitionOffsets = append(tgs.PartitionOffsets, &po)
	}

	// sort
	sort.Sort(PartitionOffsetOrderById(tgs.PartitionOffsets))

	return
}

func (this *Monitor) getGroup(zkCluster *gzk.ZkCluster,
	allGroups map[string]map[string]*gzk.ConsumerZnode,
	existedGroup map[string]struct{},
	g *Group) (err error) {

	if existedGroup == nil {
		g.ErrMsg = ErrGroupNotOnTopic.Error()
		return
	} else if _, present := existedGroup[g.Name]; !present {
		g.ErrMsg = ErrGroupNotOnTopic.Error()
		return
	}

	// get group offset and latest sub time for each partition
	// from zk path: /[kfk_root]/consumers/[group]/offsets/[topic]/
	consumerGroupOffsetOfTopicPath := zkCluster.ConsumerGroupOffsetOfTopicPath(g.Name, g.Topic)
	for partitionId, offsetData := range this.zkzone.ChildrenWithData(consumerGroupOffsetOfTopicPath) {

		var pid = int32(-1)
		if id, e := strconv.Atoi(partitionId); e == nil {
			pid = int32(id)
		}

		consumerOffset, err := strconv.ParseInt(strings.TrimSpace(string(offsetData.Data())), 10, 64)
		if err != nil {
			log.Error("kafka[%s] %s P:%s G:%s %v", zkCluster.Name(), g.Topic, partitionId, g.Name, err)
			continue
		}

		groupOffset := GroupOffset{
			Offset:    consumerOffset,
			CommitSec: int64(time.Since(offsetData.Mtime()).Seconds()),
		}

		g.Offsets[pid] = &groupOffset
	}

	// get online member if it has
	g.Members, err = this.getMember(zkCluster, allGroups, g.Topic, g.Name)
	if err != nil {
		return
	}

	if len(g.Members) != 0 {
		g.Online = true
	}

	return
}

func (this *Monitor) getMember(zkCluster *gzk.ZkCluster,
	allGroups map[string]map[string]*gzk.ConsumerZnode,
	topic, group string) (Members []*GroupMember, err error) {

	ownerByPartition := zkCluster.OwnersOfGroupByTopic(group, topic)

	for p, memId := range ownerByPartition {
		var pid = int32(-1)
		if id, e := strconv.Atoi(p); e == nil {
			pid = int32(id)
		}

		m := GroupMember{
			PartitionId: pid,
			MemId:       memId,
		}

		// find the uptime second
		if allGroups != nil {
			if allGroups[group] != nil {
				if c, present := allGroups[group][memId]; present {
					m.UpTimeSec = int64(time.Since(c.Uptime()).Seconds())
				}
			}
		}

		Members = append(Members, &m)
	}

	// sort
	sort.Sort(MemberOrderById(Members))

	return
}
