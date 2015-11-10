package zk

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

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

func TimestampToTime(ts string) time.Time {
	sec, _ := strconv.ParseInt(ts, 10, 64)
	if sec > 143761237100 {
		sec /= 1000
	}

	return time.Unix(sec, 0)
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
		path, _, err := this.conn.Get(path + zkPathSeperator + name)
		if err != nil {
			panic(err)
		}

		r[name] = path
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

func (this *ZkUtil) GetBrokers() map[string]map[string]*Broker {
	r := make(map[string]map[string]*Broker)
	for cluster, path := range this.GetClusters() {
		liveBrokers := this.getChildrenWithData(path + BrokerIdsPath)
		if len(liveBrokers) > 0 {
			r[cluster] = make(map[string]*Broker)
			for brokerId, brokerInfo := range liveBrokers {
				var broker Broker
				if err := json.Unmarshal(brokerInfo, &broker); err != nil {
					if this.conf.PanicOnError {
						panic(err)
					}
				}

				r[cluster][brokerId] = &broker
			}
		} else {
			// this cluster all brokers down?
			r[cluster] = nil
		}

	}

	return r
}

func (this *ZkUtil) GetTopics() {

}
