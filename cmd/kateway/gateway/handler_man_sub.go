package gateway

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/sla"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
	"github.com/samuel/go-zookeeper/zk"
)

// GET /v1/raw/msgs/:appid/:topic/:ver?group=xx
// tells client how to sub in raw mode: how to connect directly to kafka
func (this *manServer) subRawHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
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
		writeBadRequest(w, "illegal group")
		return
	}

	if err := manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("raw[%s] %s {topic:%s, ver:%s, hisapp:%s}: %s",
			myAppid, r.RemoteAddr, topic, ver, hisAppid, err)

		writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("cluster not found for raw subd app: %s", hisAppid)

		writeBadRequest(w, "invalid appid")
		return
	}

	log.Info("sub raw[%s] %s(%s): {app:%s, topic:%s, ver:%s group:%s}",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group)

	var out = map[string]string{
		"store": "kafka",
		"zk":    meta.Default.ZkCluster(cluster).NamedZkConnectAddr(),
		"topic": manager.Default.KafkaTopic(hisAppid, topic, ver),
		"group": myAppid + "." + group,
	}
	b, _ := json.Marshal(out)
	w.Write(b)
}

// GET /v1/peek/:appid/:topic/:ver?n=10&q=retry&wait=5s
func (this *manServer) peekHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
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
		writeBadRequest(w, "invalid appid")
		return
	}

	q := r.URL.Query()
	lastN, _ = strconv.Atoi(q.Get("n"))
	if lastN == 0 {
		// if n is not a number, lastN will be 0
		writeBadRequest(w, "invalid n param")
		return
	}
	waitParam := q.Get("wait")
	defaultWait := time.Second * 2
	var wait time.Duration = defaultWait
	if waitParam != "" {
		wait, _ = time.ParseDuration(waitParam)
		if wait.Seconds() < 1 {
			wait = defaultWait
		} else if wait.Seconds() > 5. {
			wait = defaultWait
		}
	}

	log.Info("peek[%s] %s(%s): {app:%s topic:%s ver:%s n:%d wait:%s}",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, lastN, waitParam)

	if lastN > 100 {
		lastN = 100
	}

	rawTopic = manager.Default.KafkaTopic(hisAppid, topic, ver)
	zkcluster := meta.Default.ZkCluster(cluster)

	kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		writeServerError(w, err.Error())
		return
	}
	defer kfk.Close()

	msgChan := make(chan *sarama.ConsumerMessage, 10)
	errs := make(chan error, 5)
	stopCh := make(chan struct{})

	go func() {
		partitions, err := kfk.Partitions(rawTopic)
		if err != nil {
			select {
			case errs <- err:
				return
			case <-stopCh:
				return
			}
		}

		for _, p := range partitions {
			latestOffset, err := kfk.GetOffset(rawTopic, p, sarama.OffsetNewest)
			if err != nil {
				select {
				case errs <- err:
					return
				case <-stopCh:
					return
				}
			}
			oldestOffset, err := kfk.GetOffset(rawTopic, p, sarama.OffsetOldest)
			if err != nil {
				select {
				case errs <- err:
					return
				case <-stopCh:
					return
				}
			}

			offset := latestOffset - int64(lastN)
			if offset < oldestOffset {
				offset = oldestOffset
			}

			go func(partitionId int32, offset int64) {
				consumer, err := sarama.NewConsumerFromClient(kfk)
				if err != nil {
					select {
					case errs <- err:
						return
					case <-stopCh:
						return
					}
				}
				defer consumer.Close()

				p, err := consumer.ConsumePartition(rawTopic, partitionId, offset)
				if err != nil {
					select {
					case errs <- err:
						return
					case <-stopCh:
						return
					}
				}
				defer p.Close()

				for {
					select {
					case msg := <-p.Messages():
						msgChan <- msg

					case <-stopCh:
						return
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
			close(stopCh)

			log.Error("peek[%s] %s(%s): {app:%s, topic:%s, ver:%s n:%d} %+v",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, lastN, err)

			writeServerError(w, err.Error())
			return

		case msg := <-msgChan:
			msgs = append(msgs, msg.Value)

			n++
			if n >= lastN {
				break LOOP
			}
		}
	}

	close(stopCh) // stop all the sub-goroutines

	d, _ := json.Marshal(msgs)
	w.Write(d)
}

// PUT /v1/offset/:appid/:topic/:ver/:group/:partition?offset=xx
func (this *manServer) resetSubOffsetHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
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
		realIp    = getHttpRemoteIp(r)
	)

	if !this.throttleSubStatus.Pour(realIp, 1) {
		writeQuotaExceeded(w)
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
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, partition, group, offset, err)

		writeBadRequest(w, err.Error())
		return
	}
	if offsetN < 0 {
		log.Error("sub reset offset[%s] %s(%s): {app:%s topic:%s ver:%s partition:%s group:%s offset:%s} negative offset",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, partition, group, offset)

		writeBadRequest(w, "offset must be positive")
		return
	}

	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("sub reset offset[%s] %s(%s): {app:%s topic:%s ver:%s partition:%s group:%s offset:%s} %v",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, partition, group, offset, err)

		writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("sub reset offset[%s] %s(%s): {app:%s topic:%s ver:%s partition:%s group:%s offset:%s} cluster not found",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, partition, group, offset)

		writeBadRequest(w, "invalid appid")
		return
	}

	log.Info("sub reset offset[%s] %s(%s): {app:%s topic:%s ver:%s partition:%s group:%s offset:%s}",
		myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, partition, group, offset)

	zkcluster := meta.Default.ZkCluster(cluster)
	realGroup := myAppid + "." + group
	rawTopic := manager.Default.KafkaTopic(hisAppid, topic, ver)
	err = zkcluster.ResetConsumerGroupOffset(rawTopic, realGroup, partition, offsetN)
	if err != nil {
		log.Error("sub reset offset[%s] %s(%s): {app:%s topic:%s ver:%s partition:%s group:%s offset:%s} %v",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, partition, group, offset, err)

		writeServerError(w, err.Error())
		return
	}

	w.Write(ResponseOk)
}

// DELETE /v1/groups/:appid/:topic/:ver/:group
// TODO delete shadow consumers too
func (this *manServer) delSubGroupHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
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
	realIp := getHttpRemoteIp(r)

	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("unsub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, group, err)

		writeAuthFailure(w, err)
		return
	}

	if !manager.Default.ValidateGroupName(r.Header, group) {
		log.Warn("unsub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} invalid group name",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, group)

		writeBadRequest(w, "illegal group")
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("unsub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} cluster not found",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, group)

		writeBadRequest(w, "invalid appid")
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	if group != "" {
		group = myAppid + "." + group
	}
	log.Info("unsub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} zk:%s",
		myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, group, zkcluster.ConsumerGroupRoot(group))

	if err := zkcluster.ZkZone().DeleteRecursive(zkcluster.ConsumerGroupRoot(group)); err != nil {
		log.Error("unsub[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, group, err)

		if err == zk.ErrNotEmpty {
			writeBadRequest(w, "delete a online group not allowed")
			return
		}

		writeServerError(w, err.Error())
		return
	}

	w.Write(ResponseOk)
}

// POST /v1/shadow/:appid/:topic/:ver/:group?replicas=2
func (this *manServer) addTopicShadowHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
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
	realIp := getHttpRemoteIp(r)

	if !manager.Default.ValidateGroupName(r.Header, group) {
		log.Warn("shadow+ [%s/%s] %s(%s): %s.%s.%s illegal group name", myAppid, group, r.RemoteAddr, realIp, hisAppid, topic, ver)

		writeBadRequest(w, "illegal group")
		return
	}

	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("shadow+ [%s/%s] %s(%s): %s.%s.%s %v", myAppid, group, r.RemoteAddr, realIp, hisAppid, topic, ver, err)

		writeAuthFailure(w, err)
		return
	}

	log.Info("shadow+ [%s/%s] %s(%s): %s.%s.%s", myAppid, group, r.RemoteAddr, realIp, hisAppid, topic, ver)

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("shadow+ [%s/%s] %s(%s): %s.%s.%s cluster not found", myAppid, group, r.RemoteAddr, realIp, hisAppid, topic, ver)

		writeBadRequest(w, "invalid appid")
		return
	}

	ts := sla.DefaultSla()
	query := r.URL.Query()
	ts.Replicas, err = getHttpQueryInt(&query, sla.SlaKeyReplicas, 2)
	if err != nil {
		writeBadRequest(w, err.Error())
		return
	}
	zkcluster := meta.Default.ZkCluster(cluster)
	shadowTopics := []string{
		manager.Default.ShadowTopic(sla.SlaKeyRetryTopic, myAppid, hisAppid, topic, ver, group),
		manager.Default.ShadowTopic(sla.SlaKeyDeadLetterTopic, myAppid, hisAppid, topic, ver, group),
	}
	for _, t := range shadowTopics {
		lines, err := zkcluster.AddTopic(t, ts)
		if err != nil {
			log.Error("shadow+ [%s/%s] %s(%s): %s.%s.%s %s: %s", myAppid, group, r.RemoteAddr, realIp,
				hisAppid, topic, ver, t, err.Error())

			writeServerError(w, err.Error())
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
			log.Error("shadow+ [%s/%s] %s(%s): %s.%s.%s %s: %s", myAppid, group, r.RemoteAddr, realIp,
				hisAppid, topic, ver, t, strings.Join(lines, ";"))

			writeServerError(w, strings.Join(lines, ";"))
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
	w.Write(ResponseOk)
}

// GET /v1/status/:appid/:topic/:ver?group=xx
// TODO show shadow consumers too
// response: [{"group":"group1","partition":"0","pold":0,"pubd":7827,"subd":324,"realip":"10.10.10.1"}]
func (this *manServer) subStatusHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		topic    string
		ver      string
		myAppid  string
		hisAppid string
		group    string
		err      error
		realIp   = getHttpRemoteIp(r)
	)

	if !this.throttleSubStatus.Pour(realIp, 1) {
		writeQuotaExceeded(w)
		return
	}

	query := r.URL.Query()
	group = query.Get("group") // empty means all groups
	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("sub status[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} %v",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, group, err)

		writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("sub status[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s} cluster not found",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, group)

		writeBadRequest(w, "invalid appid")
		return
	}

	log.Info("sub status[%s] %s(%s): {app:%s, topic:%s, ver:%s, group:%s}",
		myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, group)

	out, err := topicSubStatus(cluster, myAppid, hisAppid, topic, ver, group, true)
	if err != nil {
		writeServerError(w, err.Error())
		return
	}

	b, _ := json.Marshal(out)
	w.Write(b)
}

// GET /v1/subd/:topic/:ver
// response: [{"appid":"app2","group":"group1","partition":"0","pold":0,"pubd":7827,"subd":324,"realip":"192.168.10.134"},{"appid":"app2","group":"mygroup1","partition":"0","pold":0,"pubd":7827,"subd":364,"realip":""}]
func (this *manServer) subdStatusHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		topic   string
		ver     string
		myAppid string
		err     error
		realIp  = getHttpRemoteIp(r)
	)

	if !this.throttleSubStatus.Pour(realIp, 1) {
		writeQuotaExceeded(w)
		return
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	myAppid = r.Header.Get(HttpHeaderAppid)

	if err = manager.Default.OwnTopic(myAppid, r.Header.Get(HttpHeaderSubkey), topic); err != nil {
		log.Error("subd status[%s] %s(%s): {topic:%s, ver:%s} %v",
			myAppid, r.RemoteAddr, realIp, topic, ver, err)

		writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(myAppid)
	if !found {
		log.Error("subd status[%s] %s(%s): {topic:%s, ver:%s} cluster not found",
			myAppid, r.RemoteAddr, realIp, topic, ver)

		writeBadRequest(w, "invalid appid")
		return
	}

	log.Info("subd status[%s] %s(%s): {topic:%s, ver:%s}",
		myAppid, r.RemoteAddr, realIp, topic, ver)

	out, err := topicSubStatus(cluster, myAppid, myAppid, topic, ver, "", false)
	if err != nil {
		writeServerError(w, err.Error())
		return
	}

	b, _ := json.Marshal(out)
	w.Write(b)
}
