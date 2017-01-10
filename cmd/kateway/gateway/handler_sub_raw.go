package gateway

import (
	"compress/gzip"
	"net/http"
	"strconv"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
)

//go:generate goannotation $GOFILE
// @rest GET /v1/raw/msgs/:cluster/:topic?group=xx&batch=10&mux=1&reset=<newest|oldest>
func (this *subServer) subRawHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		cluster string
		topic   string
		myAppid string
		reset   string
		group   string
		limit   int // max messages to include in the message set
		err     error
	)

	if !Options.DisableMetrics {
		this.subMetrics.SubTryQps.Mark(1)
	}

	query := r.URL.Query()
	group = query.Get("group")
	reset = query.Get("reset")
	realIp := getHttpRemoteIp(r)

	if !manager.Default.ValidateGroupName(r.Header, group) {
		log.Error("sub raw -(%s): illegal group: %s", realIp, group)
		this.subMetrics.ClientError.Mark(1)
		writeBadRequest(w, "illegal group")
		return
	}

	limit, err = getHttpQueryInt(&query, "batch", 1)
	if err != nil {
		log.Error("sub raw -(%s): illegal batch: %v", realIp, err)
		this.subMetrics.ClientError.Mark(1)
		writeBadRequest(w, "illegal batch")
		return
	}
	if limit > Options.MaxSubBatchSize && Options.MaxSubBatchSize > 0 {
		limit = Options.MaxSubBatchSize
	}

	topic = params.ByName(UrlParamTopic)
	cluster = params.ByName("cluster")
	myAppid = r.Header.Get(HttpHeaderAppid)

	log.Debug("sub raw[%s/%s] %s(%s) {%s/%s batch:%d UA:%s}",
		myAppid, group, r.RemoteAddr, realIp, cluster, topic, limit, r.Header.Get("User-Agent"))

	if !Options.DisableMetrics {
		this.subMetrics.SubQps.Mark(1)
	}

	fetcher, err := store.DefaultSubStore.Fetch(cluster, topic,
		myAppid+"."+group, r.RemoteAddr, realIp, reset, Options.PermitStandbySub, query.Get("mux") == "1")
	if err != nil {
		// e,g. kafka was totally shutdown
		// e,g. too many consumers for the same group
		log.Error("sub raw[%s/%s] %s(%s) {%s/%s batch:%d UA:%s} %v",
			myAppid, group, r.RemoteAddr, realIp, cluster, topic, limit, r.Header.Get("User-Agent"), err)

		if store.DefaultSubStore.IsSystemError(err) {
			writeServerError(w, err.Error())
		} else {
			writeBadRequest(w, err.Error())
		}

		return
	}

	var gz *gzip.Writer
	w, gz = gzipWriter(w, r)
	err = this.pumpRawMessages(w, r, realIp, fetcher, limit, myAppid, topic, group)
	if err != nil {
		// e,g. broken pipe, io timeout, client gone
		// e,g. kafka: error while consuming app1.foobar.v1/0: EOF (kafka was shutdown)
		log.Error("sub raw[%s/%s] %s(%s) {%s/%s batch:%d UA:%s} %v",
			myAppid, group, r.RemoteAddr, realIp, cluster, topic, limit, r.Header.Get("User-Agent"), err)

		if err != ErrClientGone {
			if store.DefaultSubStore.IsSystemError(err) {
				writeServerError(w, err.Error())
			} else {
				writeBadRequest(w, err.Error())
			}
		}

		// fetch.Close might be called by subServer.closedConnCh
		if err = fetcher.Close(); err != nil {
			log.Error("sub raw[%s/%s] %s(%s) {%s/%s batch:%d UA:%s} %v",
				myAppid, group, r.RemoteAddr, realIp, cluster, topic, limit, r.Header.Get("User-Agent"), err)
		}
	}

	if gz != nil {
		gz.Close()
	}
}

func (this *subServer) pumpRawMessages(w http.ResponseWriter, r *http.Request, realIp string,
	fetcher store.Fetcher, limit int, myAppid, topic, group string) error {
	cn, ok := w.(http.CloseNotifier)
	if !ok {
		return ErrBadResponseWriter
	}

	var (
		n                   = 0
		idleTimeout         = Options.SubTimeout
		chunkedEver         = false
		clientGoneCh        = cn.CloseNotify()
		metaBuf      []byte = nil
	)

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

		case <-this.timer.After(idleTimeout):
			if chunkedEver {
				return nil
			}

			w.WriteHeader(http.StatusNoContent)
			w.Write([]byte{}) // without this, client cant get response
			return nil

		case err := <-fetcher.Errors():
			// e,g. consume a non-existent topic
			// e,g. conn with broker is broken
			// e,g. kafka: error while consuming foobar/0: EOF
			// e,g. kafka: error while consuming foobar/2: read tcp 10.1.1.1:60088->10.1.1.2:11005: i/o timeout
			return err

		case msg, ok := <-fetcher.Messages():
			if !ok {
				return ErrClientKilled
			}

			if limit == 1 {
				partition := strconv.FormatInt(int64(msg.Partition), 10)

				w.Header().Set("Content-Type", "text/plain; charset=utf8") // override middleware header
				w.Header().Set(HttpHeaderMsgKey, string(msg.Key))
				w.Header().Set(HttpHeaderPartition, partition)
				w.Header().Set(HttpHeaderOffset, strconv.FormatInt(msg.Offset, 10))

				// non-batch mode, just the message itself without meta
				if _, err := w.Write(msg.Value); err != nil {
					// when remote close silently, the write still ok
					return err
				}

				fetcher.CommitUpto(msg)
			} else {
				// batch mode, write MessageSet
				// MessageSet => [Partition(int32) Offset(int64) MessageSize(int32) Message] BigEndian
				if metaBuf == nil {
					// initialize the reuseable buffer
					metaBuf = make([]byte, 8)

					// override the middleware added header
					w.Header().Set("Content-Type", "application/octet-stream")
				}

				if err := writeI32(w, metaBuf, msg.Partition); err != nil {
					return err
				}
				if err := writeI64(w, metaBuf, msg.Offset); err != nil {
					return err
				}
				if err := writeI32(w, metaBuf, int32(len(msg.Value))); err != nil {
					return err
				}
				if _, err := w.Write(msg.Value); err != nil {
					return err
				}
			}

			n++
			if n >= limit {
				return nil
			}

			// http chunked: len in hex
			// curl CURLOPT_HTTP_TRANSFER_DECODING will auto unchunk
			w.(http.Flusher).Flush()

			chunkedEver = true
		}
	}
}
