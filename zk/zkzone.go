package zk

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	pt "path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/funkygao/go-simplejson"
	log "github.com/funkygao/log4go"
	"github.com/samuel/go-zookeeper/zk"
)

// ZkZone represents a single Zookeeper ensemble where many
// kafka clusters can reside each of which has a different chroot path.
type ZkZone struct {
	conf       *Config
	conn       *zk.Conn
	evt        <-chan zk.Event
	evtFetched int32
	mu         sync.RWMutex
	once       sync.Once

	errsLock sync.Mutex
	errs     []error

	zkclustersLock sync.RWMutex
	zkclusters     map[string]*ZkCluster
}

// NewZkZone creates a new ZkZone instance.
// All ephemeral nodes and watchers are automatically maintained
// event after zk connection lost and reconnected.
func NewZkZone(config *Config) *ZkZone {
	return &ZkZone{
		conf:       config,
		errs:       make([]error, 0),
		evtFetched: 0,
		zkclusters: make(map[string]*ZkCluster),
	}
}

// SessionEvents returns zk connection events.
func (this *ZkZone) SessionEvents() (<-chan zk.Event, bool) {
	this.connectIfNeccessary()

	if atomic.CompareAndSwapInt32(&this.evtFetched, 0, 1) {
		return this.evt, true
	}

	log.Warn("zk event channel being shared? NO, it's not broadcasted!")

	return nil, false
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

func (this *ZkZone) SessionTimeout() time.Duration {
	return this.conf.SessionTimeout
}

func (this *ZkZone) Ping() error {
	this.connectIfNeccessary()
	_, _, err := this.conn.Get("/") // zk sdk has no ping, simulate by Get
	return err
}

func (this *ZkZone) Close() {
	this.once.Do(func() {
		this.mu.Lock()
		if this.conn != nil {
			this.conn.Close()
			this.conn = nil
		}
		this.mu.Unlock()
	})
}

func (this *ZkZone) Conn() *zk.Conn {
	this.connectIfNeccessary()
	return this.conn
}

func (this *ZkZone) NewCluster(cluster string) *ZkCluster {
	return this.NewclusterWithPath(cluster, this.ClusterPath(cluster))
}

func (this *ZkZone) NewclusterWithPath(cluster, path string) *ZkCluster {
	this.zkclustersLock.RLock()
	c, present := this.zkclusters[cluster]
	this.zkclustersLock.RUnlock()
	if present {
		return c
	}

	this.zkclustersLock.Lock()
	if c, present := this.zkclusters[cluster]; present {
		this.zkclustersLock.Unlock()
		return c
	}

	c = &ZkCluster{
		zone:     this,
		name:     cluster,
		path:     path,
		Roster:   make([]BrokerInfo, 0),
		Replicas: 2,
		Priority: 1,
	}
	this.zkclusters[cluster] = c
	this.zkclustersLock.Unlock()

	return c
}

func (this *ZkZone) ensureParentDirExists(path string) error {
	parent := pt.Dir(path)
	if err := this.mkdirRecursive(parent); err != nil && err != zk.ErrNodeExists {
		return err
	}

	return nil
}

func (this *ZkZone) EnsurePathExists(path string) error {
	this.connectIfNeccessary()
	if err := this.mkdirRecursive(path); err != nil && err != zk.ErrNodeExists {
		return err
	}

	return nil
}

// echo -n user:passwd | openssl dgst -binary -sha1 | openssl base64
func (this *ZkZone) Auth(userColonPasswd string) error {
	this.connectIfNeccessary()
	return this.conn.AddAuth("digest", []byte(userColonPasswd))
}

func (this *ZkZone) KatewayMysqlDsn() (string, error) {
	this.connectIfNeccessary()

	data, _, err := this.conn.Get(KatewayMysqlPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return "", errors.New(fmt.Sprintf("please write mysql dsn in zk %s", KatewayMysqlPath))
		}

		return "", err
	}

	return strings.TrimSpace(string(data)), nil
}

func (this *ZkZone) KatewayJobClusterConfig() (data []byte, err error) {
	this.connectIfNeccessary()

	this.ensureParentDirExists(PubsubJobConfig)
	data, _, err = this.conn.Get(PubsubJobConfig)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, errors.New(fmt.Sprintf("please write mysql dsn in zk %s", PubsubJobConfig))
		}

		return
	}

	return
}

func (this *ZkZone) KguardInfos() ([]*KguardMeta, error) {
	this.connectIfNeccessary()

	r := make([]*KguardMeta, 0)
	data, stat, err := this.Conn().Get("/" + KguardLeaderPath)
	if err != nil {
		return nil, err
	}

	children, _, _ := this.Conn().Children("/" + KguardLeaderPath)
	r = append(r, &KguardMeta{
		Host:       string(data),
		Candidates: len(children),
		Ctime:      ZkTimestamp(stat.Ctime).Time(),
	})
	return r, nil
}

// KatewayInfos return online kateway instances meta sort by id.
func (this *ZkZone) KatewayInfos() ([]*KatewayMeta, error) {
	this.connectIfNeccessary()

	r := make([]*KatewayMeta, 0)
	path := fmt.Sprintf("%s/%s", KatewayIdsRoot, this.Name())
	katewayInstances := this.ChildrenWithData(path)
	sortedIds := make([]string, 0, len(katewayInstances))
	for id := range katewayInstances {
		sortedIds = append(sortedIds, id)
	}
	sort.Strings(sortedIds)

	for _, id := range sortedIds {
		katewayInfo := katewayInstances[id]
		var k KatewayMeta
		if err := json.Unmarshal(katewayInfo.data, &k); err != nil {
			return nil, err
		} else {
			k.Ctime = katewayInfo.Ctime()
			r = append(r, &k)
		}
	}

	return r, nil
}

func (this *ZkZone) KatewayInfoById(id string) *KatewayMeta {
	kateways, _ := this.KatewayInfos()
	for _, kw := range kateways {
		if kw.Id == id {
			return kw
		}
	}

	return nil
}

func (this *ZkZone) FlushKatewayMetrics(katewayId string, key string, data []byte) error {
	this.connectIfNeccessary()

	path := katewayMetricsRootByKey(katewayId, key)
	this.ensureParentDirExists(path)

	err := this.createZnode(path, data)
	if err == zk.ErrNodeExists {
		return this.setZnode(path, data)
	}

	return err
}

func (this *ZkZone) CreateJobQueue(topic, cluster string) error {
	this.connectIfNeccessary()

	path := fmt.Sprintf("%s/%s", PubsubJobQueues, topic)
	this.ensureParentDirExists(path)

	return this.createZnode(path, []byte(cluster))
}

func (this *Orchestrator) JobQueueCluster(topic string) (string, error) {
	this.connectIfNeccessary()

	path := fmt.Sprintf("%s/%s", PubsubJobQueues, topic)
	data, _, err := this.conn.Get(path)
	return string(data), err
}

func (this *ZkZone) CreateOrUpdateWebhook(topic string, hook WebhookMeta) error {
	this.connectIfNeccessary()

	path := fmt.Sprintf("%s/%s", PubsubWebhooks, topic)
	this.ensureParentDirExists(path)

	data := hook.Bytes()
	err := this.createZnode(path, data)
	if err == zk.ErrNodeExists {
		return this.setZnode(path, data)
	}
	return err
}

func (this *Orchestrator) WebhookInfo(topic string) (*WebhookMeta, error) {
	this.connectIfNeccessary()

	path := fmt.Sprintf("%s/%s", PubsubWebhooks, topic)
	data, _, err := this.conn.Get(path)
	if err != nil {
		return nil, err
	}

	var hook = &WebhookMeta{}
	err = hook.From(data)
	return hook, err
}

func (this *ZkZone) LoadKatewayMetrics(katewayId string, key string) ([]byte, error) {
	this.connectIfNeccessary()

	path := katewayMetricsRootByKey(katewayId, key)
	data, _, err := this.conn.Get(path)
	return data, err
}

func (this *ZkZone) swallow(path string, err error) bool {
	if err == nil {
		return true
	}

	if this.conf.PanicOnError {
		panic(err)
	}

	log.Error("%s: %v", path, err)

	this.errsLock.Lock()
	this.errs = append(this.errs, err)
	this.errsLock.Unlock()
	return false
}

func (this *ZkZone) Errors() []error {
	return this.errs
}

func (this *ZkZone) ResetErrors() {
	this.errs = make([]error, 0)
}

func (this *ZkZone) connectIfNeccessary() {
	this.mu.RLock()
	connected := this.conn != nil
	this.mu.RUnlock()

	if !connected {
		this.Connect()
	}
}

func (this *ZkZone) Connect() (err error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.conn != nil {
		log.Warn("zk %s already connected", this.conf.ZkAddrs)
		return nil
	}

	log.Debug("zk connecting %s", this.conf.ZkAddrs)
	// zk.Connect will not do real tcp connect, needn't retry here
	this.conn, this.evt, err = zk.Connect(this.ZkAddrList(), this.conf.SessionTimeout)

	return
}

func (this *ZkZone) RegisterCluster(name, path string) error {
	this.connectIfNeccessary()

	// ensure cluster root exists
	this.createZnode(clusterRoot, []byte(""))

	// create the cluster meta znode
	clusterZkPath := ClusterPath(name)
	err := this.createZnode(ClusterPath(name), []byte(path))
	if err != nil {
		return fmt.Errorf("%s: %s", clusterZkPath, err.Error())
	}

	// create the cluster kafka znode
	err = this.createZnode(path, []byte(""))
	if err == zk.ErrNodeExists {
		return nil
	}

	return errors.New(fmt.Sprintf("%s: %v", path, err))
}

func (this *ZkZone) createZnode(path string, data []byte) error {
	acl := zk.WorldACL(zk.PermAll)
	flags := int32(0)
	_, err := this.conn.Create(path, data, flags, acl)
	return err
}

func (this *ZkZone) CreateEphemeralZnode(path string, data []byte) error {
	this.connectIfNeccessary()

	if err := this.ensureParentDirExists(path); err != nil {
		return err
	}

	acl := zk.WorldACL(zk.PermAll)
	flags := int32(zk.FlagEphemeral)
	_, err := this.conn.Create(path, data, flags, acl)
	return err
}

func (this *ZkZone) CreatePermenantZnode(path string, data []byte) error {
	this.connectIfNeccessary()

	if err := this.ensureParentDirExists(path); err != nil {
		return err
	}

	acl := zk.WorldACL(zk.PermAll)
	flags := int32(0)
	_, err := this.conn.Create(path, data, flags, acl)
	return err
}

func (this *ZkZone) setZnode(path string, data []byte) error {
	_, err := this.conn.Set(path, data, -1)
	return err
}

func (this *ZkZone) children(path string) []string {
	this.connectIfNeccessary()

	children, _, err := this.conn.Children(path)
	if err != nil {
		if err != zk.ErrNoNode {
			log.Error("%s: %v", path, err)
		}

		return nil
	}

	return children
}

// return {childName: zkData}
func (this *ZkZone) ChildrenWithData(path string) map[string]zkData {
	children := this.children(path)

	r := make(map[string]zkData, len(children))
	if path == "/" {
		path = ""
	}
	for _, name := range children {
		data, stat, err := this.conn.Get(path + "/" + name)
		if err != nil {
			// e,g. /consumers/group/owners/topic/3 zk: node does not exist
			log.Error("%s: %v", path+"/"+name, err)
			continue
		}

		r[name] = zkData{
			data:  data,
			mtime: ZkTimestamp(stat.Mtime),
			ctime: ZkTimestamp(stat.Ctime),
		}
	}
	return r
}

// returns {clusterName: clusterZkPath}
func (this *ZkZone) Clusters() map[string]string {
	r := make(map[string]string)
	for cluster, clusterData := range this.ChildrenWithData(clusterRoot) {
		r[cluster] = string(clusterData.data)
	}

	return r
}

func (this *ZkZone) PublicClusters() []*ZkCluster {
	r := make([]*ZkCluster, 0)
	this.ForSortedClusters(func(c *ZkCluster) {
		if c.RegisteredInfo().Public {
			r = append(r, c)
		}
	})
	return r
}

func (this *ZkZone) CreateEsCluster(name string) error {
	this.connectIfNeccessary()
	return this.EnsurePathExists(esClusterPath(name))
}

func (this *ZkZone) NewEsCluster(name string) *EsCluster {
	if len(name) == 0 {
		panic("empty cluster name")
	}

	this.connectIfNeccessary()
	return &EsCluster{
		Name:   name,
		zkzone: this,
	}
}

func (this *ZkZone) ForSortedEsClusters(fn func(*EsCluster)) {
	this.connectIfNeccessary()

	sortedClusters := make([]string, 0)
	for _, c := range this.children(esRoot) {
		sortedClusters = append(sortedClusters, c)
	}
	sort.Strings(sortedClusters)

	for _, c := range sortedClusters {
		fn(this.NewEsCluster(c))
	}
}

func (this *ZkZone) CreateDbusCluster(name string) error {
	this.connectIfNeccessary()

	this.CreatePermenantZnode(DbusRoot, nil)
	if err := this.CreatePermenantZnode(path.Join(DbusRoot, name), nil); err != nil {
		return err
	}
	if err := this.CreatePermenantZnode(DbusCheckpointRoot(name), nil); err != nil {
		return err
	}
	if err := this.CreatePermenantZnode(DbusConfig(name), nil); err != nil {
		return err
	}
	if err := this.CreatePermenantZnode(DbusConfigDir(name), nil); err != nil {
		return err
	}
	return this.CreatePermenantZnode(DbusClusterRoot(name), nil)
}

func (this *ZkZone) DefaultDbusCluster() (cluster string) {
	this.ForSortedDbusClusters(func(name string, _ []byte) {
		cluster = name
	})
	return
}

func (this *ZkZone) ForSortedDbusClusters(fn func(name string, data []byte)) {
	this.connectIfNeccessary()
	m := make(map[string][]byte)
	for c, d := range this.ChildrenWithData(DbusRoot) {
		m[c] = d.Data()
	}
	sortedNames := make([]string, 0, len(m))
	for name := range m {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)
	for _, name := range sortedNames {
		fn(name, m[name])
	}
}

func (this *ZkZone) ForSortedClusters(fn func(zkcluster *ZkCluster)) {
	clusters := this.Clusters()
	sortedNames := make([]string, 0, len(clusters))
	for name := range clusters {
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

	path := ClusterPath(name)
	clusterPathData, _, err := this.conn.Get(path)
	if err != nil {
		log.Error("%s: %v", path, err)
		return ""
	}

	return string(clusterPathData)
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

func (this *ZkZone) DeleteRecursive(node string) (err error) {
	this.connectIfNeccessary()
	children, stat, err := this.conn.Children(node)

	if err == zk.ErrNoNode {
		return nil
	} else if err != nil {
		return
	}

	for _, child := range children {
		if err = this.DeleteRecursive(path.Join(node, child)); err != nil {
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
		if err != nil {
			log.Error("%s: %v", path+ControllerPath, err)
			continue
		}

		brokerId := js.Get("brokerid").MustInt()
		zkcluster := this.NewCluster(cluster)
		broker := zkcluster.Broker(brokerId)

		epochData, _, _ := this.conn.Get(c.controllerEpochPath())
		controller := &ControllerMeta{
			Broker: broker,
			Mtime:  ZkTimestamp(stat.Mtime),
			Epoch:  string(epochData),
		}

		r[cluster] = controller
	}
	return r
}

func (this *ZkZone) ForSortedControllers(fn func(cluster string, controller *ControllerMeta)) {
	controllers := this.controllers()
	sortedClusters := make([]string, 0, len(controllers))
	for cluster := range controllers {
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
		liveBrokers := this.ChildrenWithData(c.brokerIdsRoot())
		if len(liveBrokers) > 0 {
			r[cluster] = make(map[string]*BrokerZnode)
			for brokerId, brokerInfo := range liveBrokers {
				broker := newBrokerZnode(brokerId)
				if err := broker.from(brokerInfo.data); err != nil {
					log.Error("%s: %v", string(brokerInfo.data), err)
					continue
				}

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
	for cluster := range brokersOfClusters {
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
		"/zookeeper": {},
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

func (this *ZkZone) HostBelongs(hostIp string) (liveClusters, registeredClusters []string) {
	liveClusters = make([]string, 0)
	registeredClusters = make([]string, 0)

	// find in live brokers
	this.ForSortedBrokers(func(cluster string, liveBrokers map[string]*BrokerZnode) {
		zkcluster := this.NewCluster(cluster)

		for _, broker := range liveBrokers {
			if broker.Host == hostIp {
				liveClusters = append(liveClusters, cluster)
				break
			}
		}

		registeredBrokers := zkcluster.RegisteredInfo().Roster
		for _, broker := range registeredBrokers {
			if broker.Host == hostIp {
				registeredClusters = append(registeredClusters, cluster)
				break
			}
		}
	})

	return
}
