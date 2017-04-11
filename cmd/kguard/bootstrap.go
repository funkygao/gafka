package main

import (
	// SOS
	_ "github.com/funkygao/gafka/cmd/kguard/sos"

	// internal watchers
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/actord"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/anomaly"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/external"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/haproxy"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/influxdb"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/influxquery"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/kafka"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/kateway"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/redis"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/zk"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/zone"

	// external watchers
	_ "github.com/funkygao/dbus/watchers"
)
