package mysql

import (
	"database/sql"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	_ "github.com/funkygao/mysql"
)

type mysqlStore struct {
	cf     *config
	zkzone *zk.ZkZone

	shutdownCh chan struct{}

	// mysql store, initialized on refresh
	appClusterMap map[string]string              // appid:cluster
	appSecretMap  map[string]string              // appid:secret
	appSubMap     map[string]map[string]struct{} // appid:topics
	appPubMap     map[string]map[string]struct{} // appid:subscribed topics
}

func New(cf *config) *mysqlStore {
	if cf.Zone == "" {
		panic("empty zone")
	}
	zkAddrs := ctx.ZoneZkAddrs(cf.Zone)
	if len(zkAddrs) == 0 {
		panic("empty zookeeper addr")
	}

	return &mysqlStore{
		cf:         cf,
		zkzone:     zk.NewZkZone(zk.DefaultConfig(cf.Zone, zkAddrs)), // TODO session timeout
		shutdownCh: make(chan struct{}),
	}
}

func (this *mysqlStore) Name() string {
	return "mysql"
}

type applicationRecord struct {
	AppId, Cluster, AppSecret string
}

type appTopicRecord struct {
	AppId, TopicName string
}

type appSubscribeRecord struct {
	AppId, TopicName string
}

func (this *mysqlStore) Start() {
	if err := this.refreshFromMysql(); err != nil {
		// refuse to start if mysql conn fails
		panic(err)
	}

	go func() {
		ticker := time.NewTicker(this.cf.Refresh)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				this.refreshFromMysql()
				log.Debug("manager refreshed from mysql")

			case <-this.shutdownCh:
				log.Info("mysql manager stopped")
				return
			}
		}
	}()
}

func (this *mysqlStore) Stop() {
	close(this.shutdownCh)
}

func (this *mysqlStore) refreshFromMysql() error {
	dsn, err := this.zkzone.KatewayMysqlDsn()
	if err != nil {
		log.Error("mysql manager store fetching mysql dsn: %v", err)
		return err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Error("mysql manager store: %v", err)
		return err
	}
	defer db.Close()

	if err = this.fetchApplicationRecords(db); err != nil {
		log.Error("mysql manager store: %v", err)
		return err
	}

	if err = this.fetchTopicRecords(db); err != nil {
		log.Error("mysql manager store: %v", err)
		return err
	}

	if err = this.fetchSubscribeRecords(db); err != nil {
		log.Error("mysql manager store: %v", err)
		return err
	}

	return nil
}

func (this *mysqlStore) fetchApplicationRecords(db *sql.DB) error {
	rows, err := db.Query("SELECT AppId,Cluster,AppSecret FROM application WHERE Status=1")
	if err != nil {
		return err
	}
	defer rows.Close()

	var app applicationRecord
	appClusterMap := make(map[string]string)
	appSecretMap := make(map[string]string)
	for rows.Next() {
		err = rows.Scan(&app.AppId, &app.Cluster, &app.AppSecret)
		if err != nil {
			log.Error("mysql manager store: %v", err)
			continue
		}

		appSecretMap[app.AppId] = app.AppSecret
		appClusterMap[app.AppId] = app.Cluster
	}

	this.appClusterMap = appClusterMap
	this.appSecretMap = appSecretMap
	return nil
}

func (this *mysqlStore) fetchSubscribeRecords(db *sql.DB) error {
	rows, err := db.Query("SELECT AppId,TopicName FROM topics_subscriber WHERE Status=1")
	if err != nil {
		return err
	}
	defer rows.Close()

	var app appSubscribeRecord
	m := make(map[string]map[string]struct{})
	for rows.Next() {
		err = rows.Scan(&app.AppId, &app.TopicName)
		if err != nil {
			log.Error("mysql manager store: %v", err)
			continue
		}

		if _, present := m[app.AppId]; !present {
			m[app.AppId] = make(map[string]struct{})
		}

		m[app.AppId][app.TopicName] = struct{}{}
	}

	this.appSubMap = m

	return nil
}

func (this *mysqlStore) fetchTopicRecords(db *sql.DB) error {
	rows, err := db.Query("SELECT AppId,TopicName FROM topics WHERE Status=1")
	if err != nil {
		return err
	}
	defer rows.Close()

	var app appTopicRecord
	m := make(map[string]map[string]struct{})
	for rows.Next() {
		err = rows.Scan(&app.AppId, &app.TopicName)
		if err != nil {
			log.Error("mysql manager store: %v", err)
			continue
		}

		if _, present := m[app.AppId]; !present {
			m[app.AppId] = make(map[string]struct{})
		}

		m[app.AppId][app.TopicName] = struct{}{}
	}

	this.appPubMap = m

	return nil
}

func (this *mysqlStore) AuthPub(appid, pubkey, topic string) error {
	if appid == "" || topic == "" {
		return manager.ErrEmptyParam
	}

	// authentication
	if secret, present := this.appSecretMap[appid]; !present || pubkey != secret {
		return manager.ErrAuthenticationFail
	}

	// authorization
	if topics, present := this.appPubMap[appid]; present {
		if _, present := topics[topic]; present {
			return nil
		}
	}

	return manager.ErrAuthorizationFial
}

func (this *mysqlStore) AuthSub(appid, subkey, topic string) error {
	if appid == "" || topic == "" {
		return manager.ErrEmptyParam
	}

	// authentication
	if secret, present := this.appSecretMap[appid]; !present || subkey != secret {
		return manager.ErrAuthenticationFail
	}

	// authorization
	if topics, present := this.appSubMap[appid]; present {
		if _, present := topics[topic]; present {
			return nil
		}
	}

	return manager.ErrAuthorizationFial
}

func (this *mysqlStore) LookupCluster(appid string) (string, bool) {
	if cluster, present := this.appClusterMap[appid]; present {
		return cluster, present
	}

	return "", false
}
