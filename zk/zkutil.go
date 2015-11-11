package zk

import (
	"strconv"
	"strings"

	"github.com/funkygao/go-simplejson"
	"github.com/samuel/go-zookeeper/zk"
)

type ZkUtil struct {
	conf *Config
	conn *zk.Conn
	evt  <-chan zk.Event
}

func NewZkUtil(config *Config) *ZkUtil {
	return &ZkUtil{conf: config}
}

func (this *ZkUtil) connectIfNeccessary() {
	if this.conn == nil {
		this.Connect()
	}
}

func (this *ZkUtil) Connect() (err error) {
	this.conn, this.evt, err = zk.Connect(strings.Split(this.conf.Addrs, ","),
		this.conf.Timeout)
	if err != nil {
		if this.conf.PanicOnError {
			panic(err)
		}

		return
	}

	return
}

func (this *ZkUtil) AddCluster(name, path string) error {
	this.connectIfNeccessary()

	acl := zk.WorldACL(zk.PermAll)
	flags := int32(0)
	_, err := this.conn.Create(clusterRoot+zkPathSeperator+name, []byte(path), flags, acl)
	return err
}

func (this *ZkUtil) getChildrenWithData(path string) map[string][]byte {
	this.connectIfNeccessary()

	children, _, err := this.conn.Children(path)
	if err != nil && err != zk.ErrNoNode {
		panic(path + ":" + err.Error())
	}

	r := make(map[string][]byte)
	for _, name := range children {
		path := this.getData(path + zkPathSeperator + name)
		if path != nil {
			r[name] = path
		}
	}
	return r
}

func (this *ZkUtil) getData(path string) []byte {
	data, _, err := this.conn.Get(path)
	if err != nil {
		if this.conf.PanicOnError {
			panic(path + ":" + err.Error())
		} else {
			return nil
		}
	}

	return data
}

func (this *ZkUtil) GetTopics(cluster string) []string {
	r := make([]string, 0)
	for name, _ := range this.getChildrenWithData(clusterRoot + BrokerTopicsPath) {
		r = append(r, name)
	}
	return r

}

func (this *ZkUtil) GetClusters() map[string]string {
	r := make(map[string]string)
	for name, path := range this.getChildrenWithData(clusterRoot) {
		r[name] = string(path)
	}

	return r
}

func (this *ZkUtil) ClusterPath(name string) string {
	this.connectIfNeccessary()

	path, _, err := this.conn.Get(clusterRoot + zkPathSeperator + name)
	if err != nil {
		panic(err)
	}

	return string(path)
}

// GetControllers returns {cluster: controllerBroker}
func (this *ZkUtil) GetControllers() map[string]*Controller {
	this.connectIfNeccessary()

	r := make(map[string]*Controller)
	for cluster, path := range this.GetClusters() {
		if present, _, _ := this.conn.Exists(path + ControllerPath); !present {
			r[cluster] = nil
			continue
		}

		controllerData := this.getData(path + ControllerPath)
		js, err := simplejson.NewJson(controllerData)
		if err != nil {
			if this.conf.PanicOnError {
				panic(err)
			} else {
				continue
			}
		}

		brokerId := js.Get("brokerid").MustInt()
		broker := this.clusterBrokerIdInfo(path, brokerId)

		epochData := this.getData(path + ControllerEpochPath)
		controller := Controller{
			Broker:   broker,
			BrokerId: brokerId,
			Epoch:    string(epochData),
		}

		r[cluster] = &controller

	}
	return r
}

func (this *ZkUtil) clusterBrokerIdInfo(clusterZkPath string, id int) (b *Broker) {
	zkData := this.getData(clusterZkPath + BrokerIdsPath +
		zkPathSeperator + strconv.Itoa(id))
	b = new(Broker)
	b.from(zkData)
	return
}

func (this *ZkUtil) GetBrokersOfCluster(clusterName string) map[string]*Broker {
	clusterZkPath := this.ClusterPath(clusterName)
	r := make(map[string]*Broker)
	for brokerId, brokerInfo := range this.getChildrenWithData(clusterZkPath + BrokerIdsPath) {
		var broker Broker
		broker.from(brokerInfo)

		r[brokerId] = &broker
	}

	return r
}

// GetBrokers returns {cluster: {brokerId: broker}}
func (this *ZkUtil) GetBrokers() map[string]map[string]*Broker {
	r := make(map[string]map[string]*Broker)
	for cluster, path := range this.GetClusters() {
		liveBrokers := this.getChildrenWithData(path + BrokerIdsPath)
		if len(liveBrokers) > 0 {
			r[cluster] = make(map[string]*Broker)
			for brokerId, brokerInfo := range liveBrokers {
				var broker Broker
				broker.from(brokerInfo)

				r[cluster][brokerId] = &broker
			}
		} else {
			// this cluster all brokers down?
			r[cluster] = nil
		}

	}

	return r
}
