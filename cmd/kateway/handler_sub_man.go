package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/sla"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

type SubStatus struct {
	Appid     string `json:"appid,omitempty"`
	Group     string `json:"group"`
	Partition string `json:"partition"`
	Produced  int64  `json:"pubd"`
	Consumed  int64  `json:"subd"`
}

// GET /v1/raw/msgs/:appid/:topic/:ver?group=xx
// tells client how to sub in raw mode: how to connect directly to kafka
func (this *Gateway) subRawHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic    string
		ver      string
		hisAppid string
		myAppid  string
		group    string
	)

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	query := r.URL.Query()
	group = query.Get("group")
	if !manager.Default.ValidateGroupName(r.Header, group) {
		this.writeBadRequest(w, "illegal group")
		return
	}

	if err := manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("raw[%s] %s {topic:%s, ver:%s, hisapp:%s}: %s",
			myAppid, r.RemoteAddr, topic, ver, hisAppid, err)

		this.writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("cluster not found for raw subd app: %s", hisAppid)

		this.writeBadRequest(w, "invalid appid")
		return
	}

	var out = map[string]string{
		"store": "kafka",
		"zk":    meta.Default.ZkCluster(cluster).ZkConnectAddr(),
		"topic": manager.KafkaTopic(hisAppid, topic, ver),
		"group": group,
	}
	b, _ := json.Marshal(out)
	w.Write(b)
}

// GET /v1/peek/:appid/:topic/:ver?n=10&q=retry&wait=5s
func (this *Gateway) peekHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		myAppid  string
		hisAppid string
		topic    string
		ver      string
		lastN    int
		rawTopic string
	)

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		this.writeBadRequest(w, "invalid appid")
		return
	}

	q := r.URL.Query()
	lastN, _ = strconv.Atoi(q.Get("n"))
	if lastN == 0 {
		// if n is not a number, lastN will be 0
		this.writeBadRequest(w, "invalid n param")
		return
	}
	waitParam := q.Get("wait")
	defaultWait := time.Second * 3
	var wait time.Duration = defaultWait
	if waitParam != "" {
		wait, _ = time.ParseDuration(waitParam)
		if wait.Seconds() < 1 {
			wait = defaultWait
		} else if wait.Seconds() > 10. {
			wait = defaultWait
		}
	}

	log.Info("peek[%s] %s(%s): {app:%s, topic:%s, ver:%s n:%d}",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, lastN)

	if lastN > 100 {
		lastN = 100
	}

	rawTopic = manager.KafkaTopic(hisAppid, topic, ver)
	zkcluster := meta.Default.ZkCluster(cluster)

	kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		this.writeServerError(w, err.Error())
		return
	}

	msgChan := make(chan *sarama.ConsumerMessage, 10)
	errs := make(chan error)

	go func() {
		partitions, err := kfk.Partitions(rawTopic)
		if err != nil {
			errs <- err
			return
		}

		for _, p := range partitions {
			latestOffset, err := kfk.GetOffset(rawTopic, p, sarama.OffsetNewest)
			if err != nil {
				errs <- err
				return
			}
			oldestOffset, err := kfk.GetOffset(rawTopic, p, sarama.OffsetOldest)
			if err != nil {
				errs <- err
				return
			}

			offset := latestOffset - int64(lastN)
			if offset < oldestOffset {
				offset = oldestOffset
			}

			go func(partitionId int32, offset int64) {
				consumer, err := sarama.NewConsumerFromClient(kfk)
				if err != nil {
					errs <- err
					return
				}
				defer consumer.Close()

				p, err := consumer.ConsumePartition(rawTopic, partitionId, offset)
				if err != nil {
					errs <- err
					return
				}
				defer p.Close()

				for {
					select {
					case msg := <-p.Messages():
						msgChan <- msg
					}
				}

			}(p, offset)
		}
	}()

	n := 0
	msgs := make([][]byte, 0, lastN)
LOOP:
	for {
		select {
		case <-time.After(wait):
			break LOOP

		case err = <-errs:
			log.Error("peek[%s] %s(%s): {app:%s, topic:%s, ver:%s n:%d} %+v",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, lastN, err)

			this.writeServerError(w, err.Error())
			return

		case msg := <-msgChan:
			msgs = append(msgs, msg.Value)

			n++
			if n >= lastN {
				break LOOP
			}
		}
	}

	d, _ := json.Marshal(msgs)
	w.Write(d)
}

// GET /v1/status/:appid/:topic/:ver?group=xx
// FIXME currently there might be in flight offsets because of batch offset commit
// TODO show shadow consumers too
func (this *Gateway) subStatusHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic    string
		ver      string
		myAppid  string
		hisAppid string
		group    string
		err      error
	)

	if !this.throttleSubStatus.Pour(getHttpRemoteIp(r), 1) {
		this.writeQuotaExceeded(w)
		return
	}

	query := r.URL.Query()
	group = query.Get("group")
	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("sub status[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		this.writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("sub status[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} cluster not found",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)

		this.writeBadRequest(w, "invalid appid")
		return
	}

	log.Info("sub status[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s}",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)

	out, err := this.topicSubStatus(cluster, myAppid, hisAppid, topic, ver, group, true)
	if err != nil {
		this.writeServerError(w, err.Error())
		return
	}

	b, _ := json.Marshal(out)
	w.Write(b)
}

// PUT /v1/offset/:appid/:topic/:ver/:group/:partition?offset=xx
func (this *Gateway) resetSubOffsetHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic     string
		ver       string
		partition string
		myAppid   string
		hisAppid  string
		offset    string
		offsetN   int64
		group     string
		err       error
	)

	if !this.throttleSubStatus.Pour(getHttpRemoteIp(r), 1) {
		this.writeQuotaExceeded(w)
		return
	}

	offset = r.URL.Query().Get("offset")
	group = params.ByName(UrlParamGroup)
	partition = params.ByName("partition")
	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	offsetN, err = strconv.ParseInt(offset, 10, 64)
	if err != nil {
		log.Error("sub reset offset[%s] %s(%s): {app:%s topic:%s ver:%s partition:%s group:%s offset:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, partition, group, offset, err)

		this.writeBadRequest(w, err.Error())
		return
	}
	if offsetN < 0 {
		log.Error("sub reset offset[%s] %s(%s): {app:%s topic:%s ver:%s partition:%s group:%s offset:%s} negative offset",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, partition, group, offset)

		this.writeBadRequest(w, "offset must be positive")
		return
	}

	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("sub reset offset[%s] %s(%s): {app:%s topic:%s ver:%s partition:%s group:%s offset:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, partition, group, offset, err)

		this.writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("sub reset offset[%s] %s(%s): {app:%s topic:%s ver:%s partition:%s group:%s offset:%s} cluster not found",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, partition, group, offset)

		this.writeBadRequest(w, "invalid appid")
		return
	}

	log.Info("sub reset offset[%s] %s(%s): {app:%s topic:%s ver:%s partition:%s group:%s offset:%s}",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, partition, group, offset)

	zkcluster := meta.Default.ZkCluster(cluster)
	realGroup := myAppid + "." + group
	rawTopic := manager.KafkaTopic(hisAppid, topic, ver)
	err = zkcluster.ResetConsumerGroupOffset(rawTopic, realGroup, partition, offsetN)
	if err != nil {
		log.Error("sub reset offset[%s] %s(%s): {app:%s topic:%s ver:%s partition:%s group:%s offset:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, partition, group, offset, err)

		this.writeServerError(w, err.Error())
		return
	}

	w.Write(ResponseOk)
}

// DELETE /v1/groups/:appid/:topic/:ver/:group
// TODO delete shadow consumers too
func (this *Gateway) delSubGroupHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic    string
		ver      string
		myAppid  string
		hisAppid string
		group    string
		err      error
	)

	group = params.ByName(UrlParamGroup)
	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("unsub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		this.writeAuthFailure(w, err)
		return
	}

	if !manager.Default.ValidateGroupName(r.Header, group) {
		log.Warn("unsub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} invalid group name",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)

		this.writeBadRequest(w, "illegal group")
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("unsub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} cluster not found",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)

		this.writeBadRequest(w, "invalid appid")
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	if group != "" {
		group = myAppid + "." + group
	}
	log.Info("unsub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} zk:%s",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, zkcluster.ConsumerGroupRoot(group))

	if err := zkcluster.ZkZone().DeleteRecursive(zkcluster.ConsumerGroupRoot(group)); err != nil {
		log.Error("unsub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		this.writeServerError(w, err.Error())
		return
	}

	w.Write(ResponseOk)
}

// GET /v1/subd/:topic/:ver
func (this *Gateway) subdStatusHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic   string
		ver     string
		myAppid string
		err     error
	)

	if !this.throttleSubStatus.Pour(getHttpRemoteIp(r), 1) {
		this.writeQuotaExceeded(w)
		return
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	myAppid = r.Header.Get(HttpHeaderAppid)

	if err = manager.Default.OwnTopic(myAppid, r.Header.Get(HttpHeaderSubkey), topic); err != nil {
		log.Error("subd status[%s] %s(%s): {topic:%s, ver:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, err)

		this.writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(myAppid)
	if !found {
		log.Error("subd status[%s] %s(%s): {topic:%s, ver:%s} cluster not found",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver)

		this.writeBadRequest(w, "invalid appid")
		return
	}

	log.Info("subd status[%s] %s(%s): {topic:%s, ver:%s}",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver)

	out, err := this.topicSubStatus(cluster, myAppid, myAppid, topic, ver, "", false)
	if err != nil {
		this.writeServerError(w, err.Error())
		return
	}

	b, _ := json.Marshal(out)
	w.Write(b)
}

// POST /v1/shadow/:appid/:topic/:ver/:group
func (this *Gateway) addTopicShadowHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic    string
		ver      string
		myAppid  string
		hisAppid string
		group    string
		err      error
	)

	group = params.ByName(UrlParamGroup)
	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	if !manager.Default.ValidateGroupName(r.Header, group) {
		log.Warn("shadow sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} illegal group name",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)

		this.writeBadRequest(w, "illegal group")
		return
	}

	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("shadow sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		this.writeAuthFailure(w, err)
		return
	}

	log.Info("shadow sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s}",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("shadow sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} cluster not found",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)

		this.writeBadRequest(w, "invalid appid")
		return
	}

	ts := sla.DefaultSla()
	zkcluster := meta.Default.ZkCluster(cluster)
	shadowTopics := []string{
		manager.ShadowTopic(sla.SlaKeyRetryTopic, myAppid, hisAppid, topic, ver, group),
		manager.ShadowTopic(sla.SlaKeyDeadLetterTopic, myAppid, hisAppid, topic, ver, group),
	}
	for _, t := range shadowTopics {
		lines, err := zkcluster.AddTopic(t, ts)
		if err != nil {
			log.Error("shadow sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %s: %s",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, t, err.Error())

			this.writeServerError(w, err.Error())
			return
		}

		ok := false
		for _, l := range lines {
			if strings.Contains(l, "Created topic") {
				ok = true
				break
			}
		}

		if !ok {
			log.Error("shadow sub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %s: %s",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, t, strings.Join(lines, ";"))

			this.writeServerError(w, strings.Join(lines, ";"))
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
	w.Write(ResponseOk)
}

// a helper func
func (this *Gateway) topicSubStatus(cluster string, myAppid, hisAppid, topic, ver string,
	group string, onlyMine bool) ([]SubStatus, error) {
	zkcluster := meta.Default.ZkCluster(cluster)
	if group != "" {
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
				Group:     p[1],
				Partition: consumer.PartitionId,
				Produced:  consumer.ProducerOffset,
				Consumed:  consumer.ConsumerOffset,
			}
			if !onlyMine {
				stat.Appid = p[0]
			}

			out = append(out, stat)
		}
	}

	return out, nil
}
