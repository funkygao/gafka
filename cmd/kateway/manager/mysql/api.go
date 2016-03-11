package mysql

import (
	"github.com/funkygao/gafka/cmd/kateway/manager"
)

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
