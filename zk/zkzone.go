package zk

import (
	"container/list"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/go-simplejson"
	log "github.com/funkygao/log4go"
	"github.com/samuel/go-zookeeper/zk"
)

// ZkZone represents a single Zookeeper ensemble where many
// kafka clusters can reside each of which has a different chroot path.
type ZkZone struct {
	conf *Config
	conn *zk.Conn
	evt  <-chan zk.Event
	mu   sync.Mutex
	errs []error
}

// NewZkZone creates a new ZkZone instance.
func NewZkZone(config *Config) *ZkZone {
	return &ZkZone{
		conf: config,
		errs: make([]error, 0),
	}
}

// Name of the zone.
func (this *ZkZone) Name() string {
	return this.conf.Name
}

func (this *ZkZone) ZkAddrs() string {
	return this.conf.ZkAddrs
}

func (this *ZkZone) ZkAddrList() []string {
	return strings.Split(this.conf.ZkAddrs, ",")
}

func (this *ZkZone) Close() {
	this.conn.Close()
}

func (this *ZkZone) NewCluster(cluster string) *ZkCluster {
	return &ZkCluster{
		zone:     this,
		name:     cluster,
		path:     this.ClusterPath(cluster),
		Replicas: 2,
		Priority: 1,
	}
}

func (this *ZkZone) NewclusterWithPath(cluster, path string) *ZkCluster {
	return &ZkCluster{
		zone:     this,
		name:     cluster,
		path:     path,
		Replicas: 2,
		Priority: 1,
	}
}

func (this *ZkZone) swallow(err error) bool {
	if err != nil {
		if this.conf.PanicOnError {
			panic(err)
		}

		log.Error(err)
		this.addError(err)
		return false
	}

	return true
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
		log.Warn("zk %s already connected", this.conf.ZkAddrs)
		this.addError(ErrDupConnect)
		return nil
	}

	var i int
	for i = 1; i <= 3; i++ {
		log.Debug("zk #%d try connecting %s", i, this.conf.ZkAddrs)
		this.conn, this.evt, err = zk.Connect(this.ZkAddrList(), this.conf.Timeout)
		if err == nil {
			// connected ok
			break
		}

		backoff := time.Millisecond * 200 * time.Duration(i)
		log.Debug("zk #%d connect backoff %s", i, backoff)
		time.Sleep(backoff)
	}

	if err != nil {
		// fail fast in case of connection fail
		panic(this.conf.ZkAddrs + ":" + err.Error())
	}

	log.Debug("zk connected with %s after %d retries",
		this.conf.ZkAddrs, i-1)

	return
}

func (this *ZkZone) RegisterCluster(name, path string) error {
	this.connectIfNeccessary()

	clusterZkPath := clusterPath(name)
	err := this.createZnode(clusterPath(name), []byte(path))
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %s", clusterZkPath, err.Error())
}

func (this *ZkZone) UnregisterCluster(name string) error {
	this.connectIfNeccessary()

	return this.conn.Delete(clusterPath(name), -1)
}

func (this *ZkZone) createZnode(path string, data []byte) error {
	acl := zk.WorldACL(zk.PermAll)
	flags := int32(0)
	_, err := this.conn.Create(path, data, flags, acl)
	return err
}

func (this *ZkZone) createEphemeralZnode(path string, data []byte) error {
	acl := zk.WorldACL(zk.PermAll)
	flags := int32(zk.FlagEphemeral)
	_, err := this.conn.Create(path, data, flags, acl)
	return err
}

func (this *ZkZone) setZnode(path string, data []byte) error {
	_, err := this.conn.Set(path, data, -1)
	return err
}

func (this *ZkZone) children(path string) []string {
	this.connectIfNeccessary()

	log.Debug("zk get children: %s", path)
	children, _, err := this.conn.Children(path)
	if err != nil {
		if err != zk.ErrNoNode {
			this.swallow(err)
		}

		return nil
	}

	return children
}

// return {childName: zkData}
func (this *ZkZone) childrenWithData(path string) map[string]zkData {
	children := this.children(path)

	r := make(map[string]zkData, len(children))
	for _, name := range children {
		data, stat, err := this.conn.Get(path + "/" + name)
		if !this.swallow(err) {
			continue
		}

		r[name] = zkData{
			data:  data,
			mtime: zkTimestamp(stat.Mtime),
		}
	}
	return r
}

// returns {clusterName: clusterZkPath}
func (this *ZkZone) Clusters() map[string]string {
	r := make(map[string]string)
	for cluster, clusterData := range this.childrenWithData(clusterRoot) {
		r[cluster] = string(clusterData.data)
	}

	return r
}

func (this *ZkZone) ForSortedClusters(fn func(zkcluster *ZkCluster)) {
	clusters := this.Clusters()
	sortedNames := make([]string, 0, len(clusters))
	for name, _ := range clusters {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)
	for _, name := range sortedNames {
		c := this.NewclusterWithPath(name, clusters[name])
		fn(c)
	}
}

// ClusterPath return the zk chroot path of a cluster.
func (this *ZkZone) ClusterPath(name string) string {
	this.connectIfNeccessary()

	clusterPath, _, err := this.conn.Get(clusterPath(name))
	if err != nil {
		panic(name + ": " + err.Error())
	}

	return string(clusterPath)
}

// unused yet
func (this *ZkZone) mkdirRecursive(node string) (err error) {
	parent := path.Dir(node)
	if parent != "/" {
		if err = this.mkdirRecursive(parent); err != nil {
			return
		}
	}

	_, err = this.conn.Create(node, nil, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		err = nil
	}
	return
}

// unused yet
func (this *ZkZone) deleteRecursive(node string) (err error) {
	children, stat, err := this.conn.Children(node)
	if err == zk.ErrNoNode {
		return nil
	} else if err != nil {
		return
	}

	for _, child := range children {
		if err = this.deleteRecursive(path.Join(node, child)); err != nil {
			return
		}
	}

	return this.conn.Delete(node, stat.Version)
}

// unused yet
func (this *ZkZone) exists(path string) (ok bool, err error) {
	ok, _, err = this.conn.Exists(path)
	return
}

// returns {cluster: controllerBroker}
func (this *ZkZone) controllers() map[string]*ControllerMeta {
	this.connectIfNeccessary()

	r := make(map[string]*ControllerMeta)
	for cluster, path := range this.Clusters() {
		c := this.NewclusterWithPath(cluster, path)
		if present, _, _ := this.conn.Exists(c.controllerPath()); !present {
			r[cluster] = nil
			continue
		}

		controllerData, stat, _ := this.conn.Get(path + ControllerPath)
		js, err := simplejson.NewJson(controllerData)
		if !this.swallow(err) {
			continue
		}

		brokerId := js.Get("brokerid").MustInt()
		zkcluster := this.NewCluster(cluster)
		broker := zkcluster.Broker(brokerId)

		epochData, _, _ := this.conn.Get(c.controllerEpochPath())
		controller := &ControllerMeta{
			Broker: broker,
			Mtime:  zkTimestamp(stat.Mtime),
			Epoch:  string(epochData),
		}

		r[cluster] = controller
	}
	return r
}

func (this *ZkZone) ForSortedControllers(fn func(cluster string, controller *ControllerMeta)) {
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
func (this *ZkZone) brokers() map[string]map[string]*BrokerZnode {
	r := make(map[string]map[string]*BrokerZnode)
	for cluster, path := range this.Clusters() {
		c := this.NewclusterWithPath(cluster, path)
		liveBrokers := this.childrenWithData(c.brokerIdsRoot())
		if len(liveBrokers) > 0 {
			r[cluster] = make(map[string]*BrokerZnode)
			for brokerId, brokerInfo := range liveBrokers {
				broker := newBrokerZnode(brokerId)
				broker.from(brokerInfo.data)

				r[cluster][brokerId] = broker
			}
		} else {
			// this cluster all brokers down?
			r[cluster] = nil
		}
	}

	return r
}

func (this *ZkZone) ForSortedBrokers(fn func(cluster string, brokers map[string]*BrokerZnode)) {
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

// DiscoverClusters find all possible kafka clusters.
func (this *ZkZone) DiscoverClusters(rootPath string) ([]string, error) {
	const BROKER_PATH = "/brokers/ids"
	excludedPaths := map[string]struct{}{
		"/zookeeper": struct{}{},
	}

	result := make([]string, 0, 100)
	queue := list.New()
	queue.PushBack(rootPath)
	for {
	MAIN_LOOP:
		if queue.Len() == 0 {
			break
		}

		element := queue.Back()
		path := element.Value.(string)
		queue.Remove(element)

		// ignore the broker cluster we have already known
		for _, ignoredPath := range result {
			if strings.HasPrefix(path, ignoredPath) {
				goto MAIN_LOOP
			}
		}

		children, _, err := this.conn.Children(path)
		if err != nil {
			return nil, err
		}

		for _, child := range children {
			var p string
			if path == "/" {
				p = path + child
			} else {
				p = path + "/" + child
			}

			if _, present := excludedPaths[p]; present {
				continue
			}

			if strings.HasSuffix(p, BROKER_PATH) {
				result = append(result, p[:len(p)-len(BROKER_PATH)])

				// ignore the kafka cluster's children
				excludedPaths[p[:len(p)-len(BROKER_PATH)]] = struct{}{}
			} else {
				queue.PushBack(p)
			}
		}
	}

	return result, nil
}
