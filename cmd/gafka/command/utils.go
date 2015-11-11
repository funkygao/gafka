package command

import (
	"github.com/funkygao/gafka/zk"
)

func ensureZoneValid(zone string) {
	if _, present := cf.Zones[zone]; !present {
		panic("invalid zone: " + zone)
	}
}

func forAllZones(fn func(zone string, zkAddrs string, zkutil *zk.ZkUtil)) {
	for zone, zkAddrs := range cf.Zones {
		zkutil := zk.NewZkUtil(zk.DefaultConfig(zkAddrs))
		fn(zone, zkAddrs, zkutil)
	}
}
