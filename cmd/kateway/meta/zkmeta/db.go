package zkmeta

import (
	"database/sql"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	log "github.com/funkygao/log4go"
	_ "github.com/funkygao/mysql"
)

type applicationRecord struct {
	AppId, Cluster, AppSecret string
}

type appTopicRecord struct {
	AppId, TopicName string
}

type appSubscribeRecord struct {
	AppId, TopicName string
}

func (this *zkMetaStore) refreshFromMysql() error {
	dsn, err := this.zkzone.MysqlDsn()
	if err != nil {
		log.Error("zk meta store fetching mysql dsn: %v", err)
		return err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Error("zk meta store: %v", err)
		return err
	}
	defer db.Close()

	if err = this.fetchApplicationRecords(db); err != nil {
		log.Error("zk meta store: %v", err)
		return err
	}

	if err = this.fetchTopicRecords(db); err != nil {
		log.Error("zk meta store: %v", err)
		return err
	}

	if err = this.fetchSubscribeRecords(db); err != nil {
		log.Error("zk meta store: %v", err)
		return err
	}

	return nil
}

func (this *zkMetaStore) fetchApplicationRecords(db *sql.DB) error {
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
			log.Error("zk meta store: %v", err)
			continue
		}

		appSecretMap[app.AppId] = app.AppSecret
		appClusterMap[app.AppId] = app.Cluster
	}

	this.appClusterMap = appClusterMap
	this.appSecretMap = appSecretMap
	return nil
}

func (this *zkMetaStore) fetchSubscribeRecords(db *sql.DB) error {
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
			log.Error("zk meta store: %v", err)
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

func (this *zkMetaStore) fetchTopicRecords(db *sql.DB) error {
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
			log.Error("zk meta store: %v", err)
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

func (this *zkMetaStore) AuthPub(appid, pubkey, topic string) error {
	if appid == "" || topic == "" {
		return meta.ErrEmptyParam
	}

	// authentication
	if secret, present := this.appSecretMap[appid]; !present || pubkey != secret {
		return meta.ErrAuthenticationFail
	}

	// authorization
	if topics, present := this.appPubMap[appid]; present {
		if _, present := topics[topic]; present {
			return nil
		}
	}

	return meta.ErrAuthorizationFial
}

func (this *zkMetaStore) AuthSub(appid, subkey, topic string) error {
	if appid == "" || topic == "" {
		return meta.ErrEmptyParam
	}

	// authentication
	if secret, present := this.appSecretMap[appid]; !present || subkey != secret {
		return meta.ErrAuthenticationFail
	}

	// authorization
	if topics, present := this.appSubMap[appid]; present {
		if _, present := topics[topic]; present {
			return nil
		}
	}

	return meta.ErrAuthorizationFial
}

func (this *zkMetaStore) LookupCluster(appid string) (string, bool) {
	if cluster, present := this.appClusterMap[appid]; present {
		return cluster, present
	}

	return "", false
}
