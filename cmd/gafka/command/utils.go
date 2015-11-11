package command

import (
	"sort"

	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
)

func ensureZoneValid(zone string) {
	config.ZonePath(zone) // will panic if zone not found
}

func forAllZones(fn func(zone string, zkzone *zk.ZkZone)) {
	for _, zone := range config.SortedZones() {
		zkzone := zk.NewZkZone(zk.DefaultConfig(config.ZonePath(zone)))
		fn(zone, zkzone)
	}
}

type sortedStrMap struct {
	keys []string
	vals []interface{}
}

// TODO map[string]interface{}
func sortStrMap(m map[string]int) sortedStrMap {
	sortedKeys := make([]string, 0, len(m))
	for key, _ := range m {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	r := sortedStrMap{
		keys: sortedKeys,
		vals: make([]interface{}, len(m)),
	}
	for idx, key := range sortedKeys {
		r.vals[idx] = m[key]
	}

	return r
}
