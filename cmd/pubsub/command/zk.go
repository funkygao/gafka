package command

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/funkygao/log4go"
	"github.com/samuel/go-zookeeper/zk"
)

/*
   /_pubsub
       |
       |---bind
       |    |
       |    |---${app} {"${inbox}":"${outbox}"}
       |
       |---${app}
            |
            |---in
            |    |
            |    |--${inbox}
            |    |--${inbox}
            |
            |---out
                 |
                 |--${outbox}
                 |--${outbox}
*/

const (
	PubsubRoot   = "/_pubsub"
	BindRoot     = PubsubRoot + "/bind"
	ExchangeRoot = PubsubRoot + "/exchange"
	ZkAddr       = "localhost:2181" // TODO
)

var (
	emptyData     = []byte("")
	ErrDupConnect = errors.New("dup connect")
)

type Config struct {
	App     string
	ZkAddrs string
	Timeout time.Duration
}

func DefaultConfig(app, addrs string) *Config {
	return &Config{
		App:     app,
		ZkAddrs: addrs,
		Timeout: time.Minute,
	}
}

// Zk represents a single Zookeeper cluster where many
// kafka clusters can reside which of has a different chroot path.
type Zk struct {
	conf *Config
	conn *zk.Conn
	evt  <-chan zk.Event
	mu   sync.Mutex
	errs []error
}

// NewZk creates a new Zk instance.
func NewZk(config *Config) *Zk {
	return &Zk{
		conf: config,
		errs: make([]error, 0),
	}
}

func (this *Zk) Conn() *zk.Conn {
	this.connectIfNeccessary()
	return this.conn
}

func (this *Zk) addError(err error) {
	this.errs = append(this.errs, err)
}

func (this *Zk) connectIfNeccessary() {
	if this.conn == nil {
		this.Connect()
	}
}

func (this *Zk) Connect() (err error) {
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

func (this *Zk) Init() error {
	this.connectIfNeccessary()

	// ensure pubsub root exits
	this.createNode(PubsubRoot, emptyData)
	this.createNode(BindRoot, emptyData)

	emptyData := []byte("")
	if err := this.createNode(this.root(), emptyData); err != nil {
		return err
	}
	if err := this.createNode(this.inboxRoot(), emptyData); err != nil {
		return err
	}
	if err := this.createNode(this.outboxRoot(), emptyData); err != nil {
		return err
	}

	return nil
}

func (this *Zk) EnsureOutboxExists(topic string) {
	this.connectIfNeccessary()

	_, err := this.getData(this.outboxRoot() + "/" + topic)
	if err != nil {
		panic(fmt.Sprintf("outbox topic:%s not found", topic))
	}
}

func (this *Zk) EnsureInboxExists(topic string) {
	this.connectIfNeccessary()

	_, err := this.getData(this.inboxRoot() + "/" + topic)
	if err != nil {
		panic(fmt.Sprintf("inbox topic:%s not found", topic))
	}
}

func (this *Zk) root() string {
	return fmt.Sprintf("%s/%s", PubsubRoot, this.conf.App)
}

func (this *Zk) inboxRoot() string {
	return fmt.Sprintf("%s/in", this.root())
}

func (this *Zk) outboxRoot() string {
	return fmt.Sprintf("%s/out", this.root())
}

func (this *Zk) bindPath() string {
	return fmt.Sprintf("%s/bind/%s", PubsubRoot, this.conf.App)
}

func (this *Zk) createNode(path string, data []byte) error {
	this.connectIfNeccessary()

	acl := zk.WorldACL(zk.PermAll)
	flags := int32(0)
	log.Debug("create %s with data: %s", path, string(data))
	_, err := this.conn.Create(path, data, flags, acl)
	return err
}

func (this *Zk) setNode(path string, data []byte) error {
	this.connectIfNeccessary()

	log.Debug("set %s with data: %s", path, string(data))
	_, err := this.conn.Set(path, data, -1)
	return err
}

func (this *Zk) RegisterInbox(topic string) error {
	return this.createNode(this.inboxRoot()+"/"+topic, []byte(""))
}

func (this *Zk) Binding() (bindings map[string]string, err error) {
	this.connectIfNeccessary()

	bindings, err = this.GetJsonData(this.bindPath())
	if err == zk.ErrNoNode {
		err = this.createNode(this.bindPath(), []byte(""))
		bindings = make(map[string]string)
	}
	return
}

func (this *Zk) Bind(binding map[string]string) error {
	data, err := json.Marshal(binding)
	if err != nil {
		return err
	}

	return this.setNode(this.bindPath(), data)
}

func (this *Zk) Inboxes() []string {
	this.connectIfNeccessary()

	children, _, err := this.conn.Children(this.inboxRoot())
	if err != nil {
		panic(err)
	}

	return children
}

func (this *Zk) Outboxes() []string {
	this.connectIfNeccessary()

	children, _, err := this.conn.Children(this.outboxRoot())
	if err != nil {
		panic(err)
	}

	return children
}

func (this *Zk) RegisterOutbox(topic string) error {
	return this.createNode(this.outboxRoot()+"/"+topic, []byte(""))
}

func (this *Zk) getChildrenWithData(path string) map[string][]byte {
	this.connectIfNeccessary()

	log.Debug("get children: %s", path)
	children, _, err := this.conn.Children(path)
	if err != nil {
		if err != zk.ErrNoNode {
			log.Error(err)
			this.addError(err)
		}

		return nil
	}

	r := make(map[string][]byte, len(children))
	for _, name := range children {
		path, err := this.getData(path + "/" + name)
		if err != nil {
			log.Error(err)
			this.addError(err)
			continue
		}

		r[name] = path
	}
	return r
}

func (this *Zk) getData(path string) (data []byte, err error) {
	data, _, err = this.conn.Get(path)
	log.Debug("get data: %s -> %s", path, string(data))

	return
}

func (this *Zk) GetJsonData(path string) (map[string]string, error) {
	data, err := this.getData(path)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return make(map[string]string), nil
	}

	r := make(map[string]string)
	err = json.Unmarshal(data, &r)
	return r, err

}
