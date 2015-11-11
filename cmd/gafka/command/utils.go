package command

import (
	"sort"

	"github.com/funkygao/gafka/zk"
)

func ensureZoneValid(zone string) {
	if _, present := cf.Zones[zone]; !present {
		panic("invalid zone: " + zone)
	}
}

func forAllZones(fn func(zone string, zkutil *zk.ZkUtil)) {
	for _, zone := range cf.SortedZones() {
		zkutil := zk.NewZkUtil(zk.DefaultConfig(cf.Zones[zone]))
		fn(zone, zkutil)
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
