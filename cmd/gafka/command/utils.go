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
	zkutil := zk.NewZkUtil(zk.DefaultConfig(zkAddrs))
	for zone, zkAddrs := range cf.Zones {
		fn(zone, zkAddrs, zkutil)
	}
}
