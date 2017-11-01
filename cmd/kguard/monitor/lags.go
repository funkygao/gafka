package monitor

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
)

// POST /lag
// e,g.
// curl -XPOST -d'[{"cluster":"foo","topic":"t","group":"bar"}]' http://localhost/lags
// TODO authz
func (this *Monitor) cgLagsHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	defer func() {
		if err := recover(); err != nil {
			log.Error("%+v", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}()

	type lagRequestItem struct {
		Cluster string `json:"cluster"`
		Topic   string `json:"topic"`
		Group   string `json:"group"`
	}
	type lagRequest []lagRequestItem

	type partitionItem struct {
		Id     string `json:"id"`
		Commit int    `json:"commit"`
		Lag    int64  `json:"lag"`
	}
	type lagResponseItem struct {
		Cluster    string          `json:"cluster"`
		Topic      string          `json:"topic"`
		Group      string          `json:"group"`
		Partitions []partitionItem `json:"partitions"`
	}
	type lagResponse []lagResponseItem

	log.Trace("API[lags] from %s", r.RemoteAddr)

	remoteIP := r.RemoteAddr
	if idx := strings.Index(r.RemoteAddr, ":"); idx != -1 {
		remoteIP = r.RemoteAddr[:idx]
	}
	if !this.rl.Pour(remoteIP, 1) {
		time.Sleep(time.Second * 10) // punishment

		w.Header().Set("Connection", "close")
		http.Error(w, "lags call quota exceeded", http.StatusTooManyRequests)
		log.Error("API[lags] from %s: quota exceeded", r.RemoteAddr)
		return
	}

	dec := json.NewDecoder(r.Body)
	var req lagRequest
	err := dec.Decode(&req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// group by cluster
	res := make(lagResponse, 0, len(req))
	var clusters = make(map[string]struct{})
	for _, r := range req {
		clusters[r.Cluster] = struct{}{}
	}
	// render response
	for cluster := range clusters {
		zkcluster := this.zkzone.NewCluster(cluster)
		consumersByGroup := zkcluster.ConsumersByGroup("")
		for _, r := range req {
			if r.Cluster != cluster {
				continue
			}

			var item lagResponseItem
			item.Cluster = r.Cluster
			item.Topic = r.Topic
			item.Group = r.Group
			item.Partitions = make([]partitionItem, 0)
			for _, tp := range consumersByGroup[r.Group] {
				if tp.Topic != r.Topic {
					continue
				}

				item.Partitions = append(item.Partitions, partitionItem{
					Id:     tp.PartitionId,
					Lag:    tp.Lag,
					Commit: int(time.Since(tp.Mtime.Time()).Seconds()),
				})
			}

			res = append(res, item)
		}
	}

	b, _ := json.Marshal(res)
	w.Write(b)
}
