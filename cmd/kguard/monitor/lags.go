package monitor

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/funkygao/httprouter"
)

// POST /lag
// e,g.
// curl -XPOST -d'[{"cluster":"foo","topic":"t","group":"bar"}]' http://localhost/lags
func (this *Monitor) cgLagsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	type lagRequestItem struct {
		Cluster string `json:"cluster"`
		Topic   string `json:"topic"`
		Group   string `json:"group"`
	}
	type lagRequest []lagRequestItem

	type partitionItem struct {
		Id     string    `json:"id"`
		Uptime time.Time `json:"uptime"`
		Lag    int64     `json:"lag"`
	}
	type lagResponseItem struct {
		Cluster    string          `json:"cluster"`
		Topic      string          `json:"topic"`
		Group      string          `json:"group"`
		Partitions []partitionItem `json:"partitions"`
	}
	type lagResponse []lagResponseItem

	dec := json.NewDecoder(r.Body)
	var req lagRequest
	err := dec.Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// parse the request
	res := make(lagResponse, 0, len(req))
	var clusters map[string]struct{}
	for _, r := range req {
		clusters[r.Cluster] = struct{}{}
	}
	for cluster := range clusters {
		zkcluster := this.zkzone.NewCluster(cluster)
		consumersByGroup := zkcluster.ConsumersByGroup("")
		for _, r := range req {
			if r.Cluster != cluster {
				continue
			}

			var item lagResponseItem
			item.Cluster = r.Topic
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
					Uptime: tp.Mtime.Time(),
				})
			}

			res = append(res, item)
		}
	}

	b, _ := json.Marshal(res)
	w.Write(b)
}
