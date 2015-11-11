package zk

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/go-simplejson"
	log "github.com/funkygao/log4go"
	"github.com/samuel/go-zookeeper/zk"
)

// ZkZone represents a single Zookeeper cluster where many
// kafka clusters can reside which of has a different chroot path.
type ZkZone struct {
	conf *Config
	conn *zk.Conn
	evt  <-chan zk.Event
	mu   sync.Mutex
	errs []error
}

func (this *ZkZone) Name() string {
	return this.conf.Name
}

// NewZkZone creates a new ZkZone instance.
func NewZkZone(config *Config) *ZkZone {
	return &ZkZone{
		conf: config,
		errs: make([]error, 0),
	}
}

func (this *ZkZone) NewCluster(cluster string) *ZkCluster {
	return &ZkCluster{
		zone: this,
		name: cluster,
		path: this.clusterPath(cluster),
	}
}

func (this *ZkZone) addError(err error) {
	this.errs = append(this.errs, err)
}

func (this *ZkZone) Errors() []error {
	return this.errs
}

func (this *ZkZone) connectIfNeccessary() {
	if this.conn == nil {
		this.Connect()
	}
}

func (this *ZkZone) Connect() (err error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.conn != nil {
		log.Warn("%s already connected", this.conf.ZkAddrs)
		this.addError(ErrDupConnect)
		return nil
	}

	var i int
	for i = 1; i <= 3; i++ {
		log.Debug("#%d try connecting %s", i, this.conf.ZkAddrs)
		this.conn, this.evt, err = zk.Connect(strings.Split(this.conf.ZkAddrs, ","),
			this.conf.Timeout)
		if err == nil {
			// connected ok
			break
		}

		backoff := time.Millisecond * 200 * time.Duration(i)
		log.Debug("connect backoff %s", backoff)
		time.Sleep(backoff)
	}

	if err != nil {
		// fail fast in case of connection fail
		panic(this.conf.ZkAddrs + ":" + err.Error())
	}

	log.Debug("connected with %s after %d retries",
		this.conf.ZkAddrs, i-1)

	return
}

func (this *ZkZone) RegisterCluster(name, path string) error {
	this.connectIfNeccessary()

	acl := zk.WorldACL(zk.PermAll)
	flags := int32(0)
	_, err := this.conn.Create(clusterRoot+zkPathSeperator+name,
		[]byte(path), flags, acl)
	return err
}

func (this *ZkZone) UnregisterCluster(name string) error {
	this.connectIfNeccessary()

	return this.conn.Delete(clusterRoot+zkPathSeperator+name, -1)
}

func (this *ZkZone) getChildrenWithData(path string) map[string][]byte {
	this.connectIfNeccessary()

	log.Debug("get children: %s", path)
	children, _, err := this.conn.Children(path)
	if err != nil {
		if err != zk.ErrNoNode {
			if this.conf.PanicOnError {
				panic(path + ":" + err.Error())
			}

			log.Error(err)
			this.addError(err)
		}

		return nil
	}

	r := make(map[string][]byte, len(children))
	for _, name := range children {
		path, err := this.getData(path + zkPathSeperator + name)
		if err != nil {
			if this.conf.PanicOnError {
				panic(err)
			}

			log.Error(err)
			this.addError(err)
			continue
		}

		r[name] = path
	}
	return r
}

func (this *ZkZone) getData(path string) (data []byte, err error) {
	log.Debug("get data: %s", path)
	data, _, err = this.conn.Get(path)

	return
}

// returns {clusterName: clusterZkPath}
func (this *ZkZone) clusters() map[string]string {
	r := make(map[string]string)
	for name, path := range this.getChildrenWithData(clusterRoot) {
		r[name] = string(path)
	}

	return r
}

func (this *ZkZone) WithinClusters(fn func(name string, path string)) {
	clusters := this.clusters()
	sortedNames := make([]string, 0, len(clusters))
	for name, _ := range clusters {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)
	for _, name := range sortedNames {
		fn(name, clusters[name])
	}
}

func (this *ZkZone) clusterPath(name string) string {
	this.connectIfNeccessary()

	zkPath := clusterRoot + zkPathSeperator + name
	clusterPath, _, err := this.conn.Get(zkPath)
	if err != nil {
		if this.conf.PanicOnError {
			panic(zkPath + ":" + err.Error())
		}

		log.Error(err)
		this.addError(err)
		return ""
	}

	return string(clusterPath)
}

// returns {cluster: controllerBroker}
func (this *ZkZone) controllers() map[string]*Controller {
	this.connectIfNeccessary()

	r := make(map[string]*Controller)
	for cluster, path := range this.clusters() {
		if present, _, _ := this.conn.Exists(path + ControllerPath); !present {
			r[cluster] = nil
			continue
		}

		controllerData, _ := this.getData(path + ControllerPath)
		js, err := simplejson.NewJson(controllerData)
		if err != nil {
			if this.conf.PanicOnError {
				panic(err)
			} else {
				continue
			}
		}

		brokerId := js.Get("brokerid").MustInt()
		zkcluster := this.NewCluster(cluster)
		broker := zkcluster.Broker(brokerId)

		epochData, _ := this.getData(path + ControllerEpochPath)
		controller := &Controller{
			Broker: broker,
			Epoch:  string(epochData),
		}

		r[cluster] = controller
	}
	return r
}

func (this *ZkZone) WithinControllers(fn func(cluster string, controller *Controller)) {
	controllers := this.controllers()
	sortedClusters := make([]string, 0, len(controllers))
	for cluster, _ := range controllers {
		sortedClusters = append(sortedClusters, cluster)
	}
	sort.Strings(sortedClusters)

	for _, cluster := range sortedClusters {
		fn(cluster, controllers[cluster])
	}
}

// GetBrokers returns {cluster: {brokerId: broker}}
func (this *ZkZone) brokers() map[string]map[string]*Broker {
	r := make(map[string]map[string]*Broker)
	for cluster, path := range this.clusters() {
		liveBrokers := this.getChildrenWithData(path + BrokerIdsPath)
		if len(liveBrokers) > 0 {
			r[cluster] = make(map[string]*Broker)
			for brokerId, brokerInfo := range liveBrokers {
				broker := newBroker(brokerId)
				broker.from(brokerInfo)

				r[cluster][brokerId] = broker
			}
		} else {
			// this cluster all brokers down?
			r[cluster] = nil
		}
	}

	return r
}

func (this *ZkZone) WithinBrokers(fn func(cluster string, brokers map[string]*Broker)) {
	// sort by cluster name
	brokersOfClusters := this.brokers()
	sortedClusters := make([]string, 0, len(brokersOfClusters))
	for cluster, _ := range brokersOfClusters {
		sortedClusters = append(sortedClusters, cluster)
	}
	sort.Strings(sortedClusters)
	for _, cluster := range sortedClusters {
		fn(cluster, brokersOfClusters[cluster])
	}
}
