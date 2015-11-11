package command

import (
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
        |---${app}
             |
             |---in
             |    |
             |    |--${inbox}
             |    |--${inbox}
             |
             |---out
             |    |
             |    |--${outbox}
             |    |--${outbox}
             |
             |---bind
                  |
                  |--{"${inbox}":"${outbox}"}
*/

const (
	pubsubRoot = "/_pubsub"
)

var (
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

func (this *Zk) inboxRoot() string {
	return fmt.Sprintf("%s/%s/in", pubsubRoot, this.conf.App)
}

func (this *Zk) outboxRoot() string {
	return fmt.Sprintf("%s/%s/out", pubsubRoot, this.conf.App)
}

func (this *Zk) bindRoot() string {
	return ""
}

func (this *Zk) createNode(path string, data []byte) error {
	this.connectIfNeccessary()
	acl := zk.WorldACL(zk.PermAll)
	flags := int32(0)
	_, err := this.conn.Create(path, data, flags, acl)
	return err
}

func (this *Zk) RegisterInbox(topic string) error {
	return this.createNode(this.inboxRoot()+"/"+topic, []byte(""))
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
	log.Debug("get data: %s", path)
	data, _, err = this.conn.Get(path)

	return
}
