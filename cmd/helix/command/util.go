package command

import (
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/store/zk"
)

func getConnectedAdmin(zone string) helix.HelixAdmin {
	adm := zk.NewZkHelixAdmin(ctx.Zone(zone).ZkHelix)
	must(adm.Connect())
	return adm
}

func getConnectedManager(zone, cluster string, it helix.InstanceType) helix.HelixManager {
	m, err := zk.NewZkHelixManager(cluster, "localhost", "10009", ctx.Zone(zone).ZkHelix, it)
	must(err)
	must(m.Connect())
	return m
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
