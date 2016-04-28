package mysql

import (
	"database/sql"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	_ "github.com/funkygao/mysql"
)

type mysqlStore struct {
	cf     *config
	zkzone *zk.ZkZone

	shutdownCh chan struct{}
	refreshCh  chan struct{}

	// mysql store, initialized on refresh
	// TODO https://github.com/hashicorp/go-memdb
	appClusterMap       map[string]string              // appid:cluster
	appSecretMap        map[string]string              // appid:secret
	appSubMap           map[string]map[string]struct{} // appid:topics
	appPubMap           map[string]map[string]struct{} // appid:subscribed topics
	appConsumerGroupMap map[string]string              // appid:group
	shadowQueueMap      map[string]string              // hisappid.topic.ver.myappid:group
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
		refreshCh:  make(chan struct{}),
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

type appConsumerGroupRecord struct {
	AppId, GroupName string
}

type shadowQueueRecord struct {
	HisAppId, TopicName, Ver string
	MyAppid, Group           string
}

func (this *mysqlStore) Start() error {
	if err := this.refreshFromMysql(); err != nil {
		// refuse to start if mysql conn fails
		return err
	}

	// TODO watch KatewayMysqlDsn znode

	go func() {
		ticker := time.NewTicker(this.cf.Refresh)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				this.refreshFromMysql()
				log.Info("manager refreshed from mysql")

				select {
				case this.refreshCh <- struct{}{}:
				default:
				}

			case <-this.shutdownCh:
				log.Info("mysql manager stopped")
				return
			}
		}
	}()

	return nil
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

	// if mysql dies, keep old/stale manager records as it was
	if err = this.fetchApplicationRecords(db); err != nil {
		log.Error("apps: %v", err)
		return err
	}

	if err = this.fetchTopicRecords(db); err != nil {
		log.Error("topics: %v", err)
		return err
	}

	if err = this.fetchSubscribeRecords(db); err != nil {
		log.Error("subs: %v", err)
		return err
	}

	if err = this.fetchAppGroupRecords(db); err != nil {
		log.Error("app groups: %v", err)
		return err
	}

	if false {
		if err = this.fetchShadowQueueRecords(db); err != nil {
			log.Error("shadow queues: %v", err)
			return err
		}
	}

	return nil
}

func (this *mysqlStore) shadowKey(hisAppid, topic, ver, myAppid string) string {
	return hisAppid + "." + topic + "." + ver + "." + myAppid
}

func (this *mysqlStore) fetchShadowQueueRecords(db *sql.DB) error {
	rows, err := db.Query("SELECT HisAppId,TopicName,Version,MyAppid,GroupName FROM group_shadow WHERE Status=1")
	if err != nil {
		return err
	}
	defer rows.Close()

	var shadow shadowQueueRecord
	shadowQueueMap := make(map[string]string)
	for rows.Next() {
		err = rows.Scan(&shadow.HisAppId, &shadow.TopicName, &shadow.Ver, &shadow.MyAppid, &shadow.Group)
		if err != nil {
			log.Error("mysql manager store: %v", err)
			continue
		}

		shadowQueueMap[this.shadowKey(shadow.HisAppId, shadow.TopicName, shadow.Ver, shadow.MyAppid)] = shadow.Group
	}

	this.shadowQueueMap = shadowQueueMap
	return nil
}

func (this *mysqlStore) fetchAppGroupRecords(db *sql.DB) error {
	rows, err := db.Query("SELECT AppId,GroupName FROM application_group WHERE Status=1")
	if err != nil {
		return err
	}
	defer rows.Close()

	var group appConsumerGroupRecord
	appGroupMap := make(map[string]string)
	for rows.Next() {
		err = rows.Scan(&group.AppId, &group.GroupName)
		if err != nil {
			log.Error("mysql manager store: %v", err)
			continue
		}

		appGroupMap[group.AppId] = group.GroupName
	}

	this.appConsumerGroupMap = appGroupMap
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
	// FIXME a sub topic t, t disabled, this subscription entry should be disabled too
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
