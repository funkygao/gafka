package mysql

import (
	"net/http"
	"regexp"

	"github.com/funkygao/gafka/cmd/kateway/manager"
)

var (
	topicNameRegex = regexp.MustCompile(`[a-zA-Z0-9\-_]+`)
)

func (this *mysqlStore) Dump() map[string]interface{} {
	r := make(map[string]interface{})
	r["app_cluster"] = this.appClusterMap
	r["subscrptions"] = this.appSubMap
	r["app_topic"] = this.appPubMap
	r["groups"] = this.appConsumerGroupMap
	r["shadows"] = this.shadowQueueMap
	return r
}

func (this *mysqlStore) Refreshed() <-chan struct{} {
	return this.refreshCh
}

func (this *mysqlStore) ValidateTopicName(topic string) bool {
	return len(topic) <= 100 && len(topicNameRegex.FindAllString(topic, -1)) == 1
}

func (this *mysqlStore) ValidateGroupName(header http.Header, group string) bool {
	if len(group) == 0 {
		return false
	}

	for _, c := range group {
		if !(c == '_' || c == '-' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			return false
		}
	}

	if group == "__smoketest__" && header.Get("X-Origin") != "smoketest" {
		return false
	}

	return true
}

func (this *mysqlStore) WebHooks() ([]manager.WebHook, error) {
	return nil, nil
}

func (this *mysqlStore) AuthAdmin(appid, pubkey string) bool {
	if appid == "_psubAdmin_" && pubkey == "_wandafFan_" { // FIXME
		return true
	}

	return false
}

func (this *mysqlStore) OwnTopic(appid, pubkey, topic string) error {
	if appid == "" || topic == "" {
		return manager.ErrEmptyIdentity
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

	return manager.ErrAuthorizationFail
}

func (this *mysqlStore) AllowSubWithUnregisteredGroup(yesOrNo bool) {
	this.allowUnregisteredGroup = yesOrNo
}

func (this *mysqlStore) AuthSub(appid, subkey, hisAppid, hisTopic, group string) error {
	if appid == "" || hisTopic == "" {
		return manager.ErrEmptyIdentity
	}

	// authentication
	if secret, present := this.appSecretMap[appid]; !present || subkey != secret {
		return manager.ErrAuthenticationFail
	}

	// group verification
	if !this.allowUnregisteredGroup {
		if group == "" {
			// empty group, means we skip group verification
		} else if group != "__smoketest__" {
			if _, present := this.appConsumerGroupMap[appid][group]; !present {
				return manager.ErrInvalidGroup
			}
		}
	}

	if appid == hisAppid {
		// sub my own topic is always authorized FIXME what if the topic is disabled?
		return nil
	}

	// authorization
	if topics, present := this.appSubMap[appid]; present {
		if _, present := topics[hisTopic]; present {
			return nil
		}
	}

	return manager.ErrAuthorizationFail
}

func (this *mysqlStore) LookupCluster(appid string) (string, bool) {
	if cluster, present := this.appClusterMap[appid]; present {
		return cluster, present
	}

	return "", false
}

func (this *mysqlStore) IsShadowedTopic(hisAppid, topic, ver, myAppid, group string) bool {
	if _, present := this.shadowQueueMap[this.shadowKey(hisAppid, topic, ver, myAppid)]; present {
		return true
	}

	return false
}
