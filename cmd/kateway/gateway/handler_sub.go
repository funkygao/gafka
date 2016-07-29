package gateway

import (
	"net/http"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/sla"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
)

// GET /v1/msgs/:appid/:topic/:ver?group=xx&batch=10&reset=<newest|oldest>&ack=1&q=<dead|retry>
func (this *subServer) subHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		topic      string
		ver        string
		myAppid    string
		hisAppid   string
		reset      string
		group      string
		shadow     string
		rawTopic   string
		partition  string
		partitionN int = -1
		offset     string
		offsetN    int64 = -1
		limit      int   // max messages to include in the message set
		delayedAck bool  // explicit application level acknowledgement
		err        error
	)

	if Options.EnableClientStats {
		this.gw.clientStates.RegisterSubClient(r)
	}

	query := r.URL.Query()
	group = query.Get("group")
	reset = query.Get("reset")
	if !manager.Default.ValidateGroupName(r.Header, group) {
		this.subMetrics.ClientError.Mark(1)
		writeBadRequest(w, "illegal group")
		return
	}

	limit, err = getHttpQueryInt(&query, "batch", 1)
	if err != nil {
		this.subMetrics.ClientError.Mark(1)
		writeBadRequest(w, "illegal limit")
		return
	}
	if limit > Options.MaxSubBatchSize && Options.MaxSubBatchSize > 0 {
		limit = Options.MaxSubBatchSize
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)
	realIp := getHttpRemoteIp(r)

	// auth
	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("sub[%s] -(%s): {app:%s topic:%s ver:%s group:%s UA:%s} %v",
			myAppid, realIp, hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), err)

		this.subMetrics.ClientError.Mark(1)
		writeAuthFailure(w, err)
		return
	}

	// fetch the client ack partition and offset
	delayedAck = query.Get("ack") == "1"
	if delayedAck {
		// consumers use explicit acknowledges in order to signal a message as processed successfully
		// if consumers fail to ACK, the message hangs and server will refuse to move ahead

		// get the partitionN and offsetN from client header
		// client will ack with partition=-1, offset=-1:
		// 1. handshake phase
		// 2. when 204 No Content
		partition = r.Header.Get(HttpHeaderPartition)
		offset = r.Header.Get(HttpHeaderOffset)
		if partition != "" && offset != "" {
			// convert partition and offset to int
			offsetN, err = strconv.ParseInt(offset, 10, 64)
			if err != nil {
				log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} offset:%s",
					myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver,
					group, r.Header.Get("User-Agent"), offset)

				this.subMetrics.ClientError.Mark(1)
				writeBadRequest(w, "ack with bad offset")
				return
			}
			partitionN, err = strconv.Atoi(partition)
			if err != nil {
				log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} partition:%s",
					myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver,
					group, r.Header.Get("User-Agent"), partition)

				this.subMetrics.ClientError.Mark(1)
				writeBadRequest(w, "ack with bad partition")
				return
			}
		} else if len(partition+offset) != 0 {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s partition:%s offset:%s UA:%s} partial ack",
				myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver,
				group, partition, offset, r.Header.Get("User-Agent"))

			this.subMetrics.ClientError.Mark(1)
			writeBadRequest(w, "partial ack not allowed")
			return
		}
	}

	shadow = query.Get("q")

	if Options.AuditSub {
		this.auditor.Trace("sub[%s] %s(%s): {app:%s q:%s topic:%s ver:%s group:%s batch:%d ack:%s partition:%s offset:%s UA:%s}",
			myAppid, r.RemoteAddr, realIp, hisAppid, shadow, topic, ver,
			group, limit, query.Get("ack"), partition, offset, r.Header.Get("User-Agent"))
	}

	if !Options.DisableMetrics {
		this.subMetrics.SubQps.Mark(1)
	}

	// calculate raw topic according to shadow
	if shadow != "" {
		if !sla.ValidateShadowName(shadow) {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s q:%s UA:%s} invalid shadow name",
				myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver,
				group, shadow, r.Header.Get("User-Agent"))

			this.subMetrics.ClientError.Mark(1)
			writeBadRequest(w, "invalid shadow name")
			return
		}

		if !manager.Default.IsShadowedTopic(hisAppid, topic, ver, myAppid, group) {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s q:%s UA:%s} not a shadowed topic",
				myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver,
				group, shadow, r.Header.Get("User-Agent"))

			this.subMetrics.ClientError.Mark(1)
			writeBadRequest(w, "register shadow first")
			return
		}

		rawTopic = manager.Default.ShadowTopic(shadow, myAppid, hisAppid, topic, ver, group)
	} else {
		rawTopic = manager.Default.KafkaTopic(hisAppid, topic, ver)
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} cluster not found",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"))

		this.subMetrics.ClientError.Mark(1)
		writeBadRequest(w, "invalid appid")
		return
	}

	fetcher, err := store.DefaultSubStore.Fetch(cluster, rawTopic,
		myAppid+"."+group, r.RemoteAddr, reset, Options.PermitStandbySub)
	if err != nil {
		log.Error("sub[%s] -(%s): {app:%s topic:%s ver:%s group:%s UA:%s} %v",
			myAppid, realIp, hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), err)

		if store.DefaultSubStore.IsSystemError(err) {
			writeServerError(w, err.Error())
		} else {
			this.subMetrics.ClientError.Mark(1)
			writeBadRequest(w, err.Error())
		}

		return
	}

	// commit the acked offset
	if delayedAck && partitionN >= 0 && offsetN >= 0 {
		if err = fetcher.CommitUpto(&sarama.ConsumerMessage{
			Topic:     rawTopic,
			Partition: int32(partitionN),
			Offset:    offsetN,
		}); err != nil {
			// during rebalance, this might happen, but with no bad effects
			log.Trace("sub land[%s] %s(%s): {app:%s topic:%s/%s ver:%s group:%s ack:1 offset:%s UA:%s} %v",
				myAppid, r.RemoteAddr, realIp, hisAppid, topic, partition, ver,
				group, offset, r.Header.Get("User-Agent"), err)
		} else {
			log.Debug("sub land %s(%s): {G:%s, T:%s/%s, O:%s}",
				r.RemoteAddr, realIp, group, rawTopic, partition, offset)
		}
	}

	err = this.pumpMessages(w, r, fetcher, limit, myAppid, hisAppid, topic, ver, group, delayedAck)
	if err != nil {
		// e,g. broken pipe, io timeout, client gone
		log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s ack:%s partition:%s offset:%s UA:%s} %v",
			myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver,
			group, query.Get("ack"), partition, offset, r.Header.Get("User-Agent"), err)

		writeServerError(w, err.Error())

		if err = fetcher.Close(); err != nil {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s} %v",
				myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, group, err)
		}
	}
}

func (this *subServer) pumpMessages(w http.ResponseWriter, r *http.Request,
	fetcher store.Fetcher, limit int, myAppid, hisAppid, topic, ver, group string, delayedAck bool) error {
	cn, ok := w.(http.CloseNotifier)
	if !ok {
		return ErrBadResponseWriter
	}

	var (
		metaBuf       []byte = nil
		n                    = 0
		idleTimeout          = Options.SubTimeout
		realIp               = getHttpRemoteIp(r)
		chunkedEver          = false
		tagConditions        = make(map[string]struct{})
		clientGoneCh         = cn.CloseNotify()
		startedAt            = time.Now()
	)

	// parse http tag header as filter condition
	if tagFilter := r.Header.Get(HttpHeaderMsgTag); tagFilter != "" {
		for _, t := range parseMessageTag(tagFilter) {
			if t != "" {
				tagConditions[t] = struct{}{}
			}
		}
	}

	for {
		if time.Since(startedAt) > idleTimeout {
			// e,g. tag filter got 1000 msgs, but no tag hit after timeout, we'll return 204
			if chunkedEver {
				return nil
			}

			w.WriteHeader(http.StatusNoContent)
			w.Write([]byte{})
			return nil
		}

		select {
		case <-clientGoneCh:
			// FIXME access log will not be able to record this behavior
			return ErrClientGone

		case <-this.gw.shutdownCh:
			// don't call me again
			w.Header().Set("Connection", "close")

			if !chunkedEver {
				w.WriteHeader(http.StatusNoContent)
				w.Write([]byte{})
			}

			return nil

		case err := <-fetcher.Errors():
			// e,g. consume a non-existent topic
			// e,g. conn with broker is broken
			// e,g. kafka: error while consuming foobar/0: EOF
			// e,g. kafka: error while consuming foobar/2: read tcp 10.1.1.1:60088->10.1.1.2:11005: i/o timeout
			return err

		case <-this.gw.timer.After(idleTimeout):
			if chunkedEver {
				// response already sent in chunk
				log.Debug("chunked sub idle timeout %s {A:%s/G:%s->A:%s T:%s V:%s}",
					idleTimeout, myAppid, group, hisAppid, topic, ver)
				return nil
			}

			w.WriteHeader(http.StatusNoContent)
			w.Write([]byte{}) // without this, client cant get response
			return nil

		case msg, ok := <-fetcher.Messages():
			if !ok {
				return ErrClientKilled
			}

			if Options.Debug {
				log.Debug("sub[%s] %s(%s): {G:%s T:%s/%d O:%d}",
					myAppid, r.RemoteAddr, realIp, group, msg.Topic, msg.Partition, msg.Offset)
			}

			partition := strconv.FormatInt(int64(msg.Partition), 10)

			if limit == 1 {
				w.Header().Set(HttpHeaderMsgKey, string(msg.Key))
				w.Header().Set(HttpHeaderPartition, partition)
				w.Header().Set(HttpHeaderOffset, strconv.FormatInt(msg.Offset, 10))
			}

			var (
				tags    []string
				bodyIdx int
				err     error
			)
			if IsTaggedMessage(msg.Value) {
				tags, bodyIdx, err = ExtractMessageTag(msg.Value)
				if err != nil {
					log.Error("sub[%s] %s(%s): {G:%s T:%s/%d O:%d} %v",
						myAppid, r.RemoteAddr, realIp, group, msg.Topic, msg.Partition, msg.Offset, err)

					if !delayedAck {
						fetcher.CommitUpto(msg)
					}

					continue
				}
			}

			// assert tag conditions are satisfied. if empty, feed all messages
			if len(tagConditions) > 0 {
				tagSatisfied := false
				for _, t := range tags {
					if _, present := tagConditions[t]; present {
						tagSatisfied = true
						break
					}
				}

				if !tagSatisfied {
					if !delayedAck {
						log.Debug("sub auto commit offset with tag unmatched %s(%s): {G:%s, T:%s/%d, O:%d} %+v/%+v",
							r.RemoteAddr, realIp, group, msg.Topic, msg.Partition, msg.Offset, tagConditions, tags)

						fetcher.CommitUpto(msg)
					}

					continue
				}
			}

			if limit == 1 {
				// non-batch mode, just the message itself without meta
				if _, err = w.Write(msg.Value[bodyIdx:]); err != nil {
					// when remote close silently, the write still ok
					return err
				}
			} else {
				// batch mode, write MessageSet
				// MessageSet => [Partition(int32) Offset(int64) MessageSize(int32) Message] BigEndian
				if metaBuf == nil {
					// initialize the reuseable buffer
					metaBuf = make([]byte, 8)

					// remove the middleware added header
					w.Header().Del("Content-Type")
				}

				if err = writeI32(w, metaBuf, msg.Partition); err != nil {
					return err
				}
				if err = writeI64(w, metaBuf, msg.Offset); err != nil {
					return err
				}
				if err = writeI32(w, metaBuf, int32(len(msg.Value[bodyIdx:]))); err != nil {
					return err
				}
				if _, err = w.Write(msg.Value[bodyIdx:]); err != nil {
					return err
				}
			}

			if !delayedAck {
				log.Debug("sub auto commit offset %s(%s): {G:%s, T:%s/%d, O:%d}",
					r.RemoteAddr, realIp, group, msg.Topic, msg.Partition, msg.Offset)

				// ignore the offset commit err on purpose:
				// during rebalance, offset commit often encounter errors because fetcher
				// underlying partition offset tracker has changed
				// e,g.
				// topic has partition: 0, 1
				// 1. got msg(p=0) from fetcher
				// 2. rebalanced, then start consuming p=1
				// 3. commit the msg offset, still msg(p=0) => error
				// BUT, it has no fatal effects.
				// The worst case is between 1-3, kateway shutdown, sub client
				// will get 1 duplicated msg.
				fetcher.CommitUpto(msg)
			} else {
				log.Debug("sub take off %s(%s): {G:%s, T:%s/%d, O:%d}",
					r.RemoteAddr, realIp, group, msg.Topic, msg.Partition, msg.Offset)
			}

			this.subMetrics.ConsumeOk(myAppid, topic, ver)
			this.subMetrics.ConsumedOk(hisAppid, topic, ver)

			n++
			if n >= limit {
				return nil
			}

			// http chunked: len in hex
			// curl CURLOPT_HTTP_TRANSFER_DECODING will auto unchunk
			w.(http.Flusher).Flush()

			chunkedEver = true

			if n == 1 {
				log.Debug("sub idle timeout %s->1s %s(%s): {G:%s, T:%s/%d, O:%d B:%d}",
					idleTimeout, r.RemoteAddr, realIp, group, msg.Topic, msg.Partition, msg.Offset, limit)
				idleTimeout = time.Second
			}

		}
	}
}
