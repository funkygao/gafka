package monitor

import (
	"encoding/json"
	"fmt"
	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
	"net/http"
	"sort"
	"strconv"
)

type Broker struct {
	Id         int    `json:"id"`
	IP         string `json:"ip"`
	Port       int    `json:"port"`
	DNS        string `json:"dns"`
	Registered bool   `json:"registerd"`
	Online     bool   `json:"online"`
	ErrMsg     string `json:"error"`
}

type Cluster struct {
	Name    string    `json:"cluster_name"`
	ZkAddr  string    `json:"zk_addr"`
	ZkPath  string    `json:"zk_root_path"`
	Brokers []*Broker `json:"brokers"`
	ErrMsg  string    `json:"error"`
}

type Clusters struct {
	All []*Cluster `json:"clusters"`
}

type BrokerSortById []*Broker

func (this BrokerSortById) Len() int           { return len(this) }
func (this BrokerSortById) Less(i, j int) bool { return this[i].Id < this[j].Id }
func (this BrokerSortById) Swap(i, j int)      { this[i], this[j] = this[j], this[i] }

// @rest POST /kfk/clusters
// 1. how many clusters
// 2. how many brokers in each cluster
// 3. broker online or not
// 4. broker registered or not
// 5. broker's dns
// 6. if clusters is not null, get specific cluster info, if clusters not exist, return errmsg
// 7. if brokers is not null, get specific broker info by broker id, if broker id not exist , return errmsg
// eg: curl -XPOST http://192.168.149.150:10025/kfk/clusters -d'[{"cluster":"bigtopic", "brokers":[0,1]}]'
// TODO authz and rate limitation
func (this *Monitor) kfkClustersHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	defer func() {
		if err := recover(); err != nil {
			log.Error("%+v", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}()

	type clusterRequestItem struct {
		Cluster string `json:"cluster"`
		Brokers []int  `json:"brokers"`
	}

	type clusterRequest []clusterRequestItem

	dec := json.NewDecoder(r.Body)
	var req clusterRequest
	err := dec.Decode(&req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	reqInfo := make(map[string][]int) // cluster --> brokerList --> [0,1,2]
	for _, c := range req {
		reqInfo[c.Cluster] = c.Brokers
	}

	clusters, err := this.getClusters(reqInfo)
	if err != nil {
		log.Error("clusters:%v %v", reqInfo, err)
		writeServerError(w, err.Error())
		return
	}

	b, _ := json.Marshal(clusters)
	w.Write(b)
	return

}

func (this *Monitor) getClusters(reqClusters map[string][]int) (clusters Clusters, err error) {

	zkZone := this.zkzone
	allClusters := zkZone.Clusters()
	zkAddr := zkZone.ZkAddrs()

	// preallocate all slot
	if len(reqClusters) == 0 {
		// we need to get all clusters
		for cn, path := range allClusters {
			c := Cluster{
				Name:   cn,
				ZkAddr: zkAddr,
				ZkPath: path,
			}

			clusters.All = append(clusters.All, &c)
		}
	} else {
		// we need to get target clusters
		for reqC, _ := range reqClusters {
			c := Cluster{
				Name: reqC,
			}
			// check cluster exist or not
			if path, ok := allClusters[reqC]; !ok {
				c.ErrMsg = ErrClusterNotExist.Error()
			} else {
				c.ZkAddr = zkAddr
				c.ZkPath = path
			}

			clusters.All = append(clusters.All, &c)
		}
	}

	for _, c := range clusters.All {
		if c.ErrMsg == "" { // valid cluster
			reqBrokers, _ := reqClusters[c.Name]
			err = this.getCluster(c, reqBrokers)
			if err != nil {
				return
			}
		}
	}

	return
}

func (this *Monitor) getCluster(c *Cluster, reqBrokers []int) (err error) {

	zkCluster := this.zkzone.NewCluster(c.Name)
	onlineBrokers := zkCluster.Brokers()
	regBrokerList := zkCluster.RegisteredInfo().Roster
	regBrokers := make(map[int]*gzk.BrokerInfo)
	allBrokers := make(map[int]struct{})
	var null struct{}

	for i, _ := range regBrokerList {
		var b = regBrokerList[i]
		regBrokers[b.Id] = &b
	}

	// get all brokers' set from registered and online
	// traverse registered list
	for bId, _ := range regBrokers {
		allBrokers[bId] = null
	}

	// traverse online list
	for id, _ := range onlineBrokers {
		bId := -1 // default invalid broker id
		i, e := strconv.Atoi(id)
		if e == nil {
			bId = i
		}

		allBrokers[bId] = null
	}

	var realReqBrokers []int
	if len(reqBrokers) == 0 {
		// all brokers
		for bId, _ := range allBrokers {
			realReqBrokers = append(realReqBrokers, bId)
		}
	} else {
		realReqBrokers = reqBrokers
	}

	// traverse real req brokers and find the info
	for _, bId := range realReqBrokers {
		var isReg, isOnline bool
		var onlineBroker *gzk.BrokerZnode
		var regBroker *gzk.BrokerInfo

		newBroker := Broker{
			Id: bId,
		}

		if regBroker, isReg = regBrokers[bId]; isReg {
			newBroker.IP = regBroker.Host
			newBroker.Port = regBroker.Port
			newBroker.Registered = true
		}

		if onlineBroker, isOnline = onlineBrokers[fmt.Sprintf("%d", bId)]; isOnline {
			newBroker.IP = onlineBroker.Host
			newBroker.Port = onlineBroker.Port
			newBroker.Online = true
		}

		if isReg != true && isOnline != true {
			newBroker.ErrMsg = ErrBrokerIdNotExist.Error()
		} else {
			if dns, present := ctx.ReverseDnsLookup(newBroker.IP, newBroker.Port); present {
				newBroker.DNS = dns
			}
		}

		c.Brokers = append(c.Brokers, &newBroker)
	}

	// sort by brokerId
	sort.Sort(BrokerSortById(c.Brokers))

	return
}
