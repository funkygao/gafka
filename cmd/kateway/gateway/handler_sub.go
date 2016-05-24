package gateway

import (
	"net/http"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/sla"
	"github.com/funkygao/golib/hack"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// GET /v1/msgs/:appid/:topic/:ver?group=xx&batch=10&wait=5s&reset=<newest|oldest>&ack=1&q=<dead|retry>
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
		offsetN    int64         = -1
		limit      int           // max messages to include in the message set
		wait       time.Duration // max time to wait if insufficient data is available
		delayedAck bool          // explicit application level acknowledgement
		tagFilters []MsgTag      = nil
		err        error
	)

	if Options.EnableClientStats {
		this.gw.clientStates.RegisterSubClient(r)
	}

	query := r.URL.Query()
	group = query.Get("group")
	reset = query.Get("reset")
	if !manager.Default.ValidateGroupName(r.Header, group) {
		writeBadRequest(w, "illegal group")
		return
	}

	limit, err = getHttpQueryInt(&query, "batch", 1)
	if err != nil {
		writeBadRequest(w, "illegal limit")
		return
	}
	if limit > Options.MaxSubBatchSize && Options.MaxSubBatchSize > 0 {
		limit = Options.MaxSubBatchSize
	}

	wait, err = time.ParseDuration(query.Get("wait"))
	if err != nil || wait < MinSubWait {
		wait = MinSubWait
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	// auth
	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), err)

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
					myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
					group, r.Header.Get("User-Agent"), offset)

				writeBadRequest(w, "ack with bad offset")
				return
			}
			partitionN, err = strconv.Atoi(partition)
			if err != nil {
				log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} partition:%s",
					myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
					group, r.Header.Get("User-Agent"), partition)

				writeBadRequest(w, "ack with bad partition")
				return
			}
		} else if len(partition+offset) != 0 {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s partition:%s offset:%s UA:%s} partial ack",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, partition, offset, r.Header.Get("User-Agent"))

			writeBadRequest(w, "partial ack not allowed")
			return
		}
	}

	shadow = query.Get("q")

	log.Debug("sub[%s] %s(%s): {app:%s q:%s topic:%s ver:%s group:%s batch:%d ack:%s partition:%s offset:%s UA:%s}",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, shadow, topic, ver,
		group, limit, query.Get("ack"), partition, offset, r.Header.Get("User-Agent"))

	// calculate raw topic according to shadow
	if shadow != "" {
		if !sla.ValidateShadowName(shadow) {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s q:%s UA:%s} invalid shadow name",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, shadow, r.Header.Get("User-Agent"))

			writeBadRequest(w, "invalid shadow name")
			return
		}

		if !manager.Default.IsShadowedTopic(hisAppid, topic, ver, myAppid, group) {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s q:%s UA:%s} not a shadowed topic",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, shadow, r.Header.Get("User-Agent"))

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
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"))

		writeBadRequest(w, "invalid appid")
		return
	}

	fetcher, err := store.DefaultSubStore.Fetch(cluster, rawTopic,
		myAppid+"."+group, r.RemoteAddr, reset, Options.PermitStandbySub)
	if err != nil {
		log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), err)

		writeBadRequest(w, err.Error())
		return
	}

	// commit the acked offset
	if delayedAck && partitionN >= 0 && offsetN >= 0 {
		// what if shutdown kateway now?
		// the commit will be ok, and when pumpMessages, the conn will get http.StatusNoContent
		if err = fetcher.CommitUpto(&sarama.ConsumerMessage{
			Topic:     rawTopic,
			Partition: int32(partitionN),
			Offset:    offsetN,
		}); err != nil {
			log.Error("sub commit[%s] %s(%s): {app:%s topic:%s ver:%s group:%s ack:1 partition:%s offset:%s UA:%s} %v",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, partition, offset, r.Header.Get("User-Agent"), err)

			writeBadRequest(w, err.Error())
			return
		} else {
			log.Debug("sub land %s(%s): {G:%s, T:%s, P:%s, O:%s}",
				r.RemoteAddr, getHttpRemoteIp(r),
				group, rawTopic, partition, offset)
		}
	}

	tag := r.Header.Get(HttpHeaderMsgTag)
	if tag != "" {
		tagFilters = parseMessageTag(tag)
	}

	err = this.pumpMessages(w, r, fetcher, limit, wait, myAppid, hisAppid, topic, ver, group, delayedAck, tagFilters)
	if err != nil {
		// e,g. broken pipe, io timeout, client gone
		log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s ack:%s partition:%s offset:%s UA:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, query.Get("ack"), partition, offset, r.Header.Get("User-Agent"), err)

		writeServerError(w, err.Error())

		if err = fetcher.Close(); err != nil {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s} %v",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)
		}
	}
}

func (this *subServer) pumpMessages(w http.ResponseWriter, r *http.Request,
	fetcher store.Fetcher, limit int, wait time.Duration, myAppid, hisAppid, topic, ver,
	group string, delayedAck bool, tagFilters []MsgTag) error {
	clientGoneCh := w.(http.CloseNotifier).CloseNotify()

	var metaBuf []byte = nil
	n := 0
	idleTimeout := Options.SubTimeout
	chunkedEver := false
	for {
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

		case msg := <-fetcher.Messages():
			partition := strconv.FormatInt(int64(msg.Partition), 10)

			if limit == 1 {
				w.Header().Set(HttpHeaderMsgKey, string(msg.Key))
				w.Header().Set(HttpHeaderPartition, partition)
				w.Header().Set(HttpHeaderOffset, strconv.FormatInt(msg.Offset, 10))
			}

			var (
				tags    []MsgTag
				bodyIdx int
				err     error
			)
			if IsTaggedMessage(msg.Value) {
				// TagMarkStart + tag + TagMarkEnd + body
				tags, bodyIdx, err = ExtractMessageTag(msg.Value)
				if limit == 1 && err == nil {
					// needn't check 'index out of range' here
					w.Header().Set(HttpHeaderMsgTag, hack.String(msg.Value[1:bodyIdx-1]))
				} else {
					// not a valid tagged message, treat it as non-tagged message
				}
			}

			if len(tags) > 0 {
				// TODO compare with tagFilters
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
				// TODO add tag?
				if _, err = w.Write(msg.Value[bodyIdx:]); err != nil {
					return err
				}
			}

			if !delayedAck {
				log.Debug("sub commit offset {G:%s, T:%s, P:%d, O:%d}", group, msg.Topic, msg.Partition, msg.Offset)
				if err = fetcher.CommitUpto(msg); err != nil {
					return err
				}
			} else {
				log.Debug("sub take off %s(%s): {G:%s, T:%s, P:%d, O:%d}",
					r.RemoteAddr, getHttpRemoteIp(r),
					group, msg.Topic, msg.Partition, msg.Offset)
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
			idleTimeout = wait // user specified wait only works after recv 1st message in batch
		}
	}
}
