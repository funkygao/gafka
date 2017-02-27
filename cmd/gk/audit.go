package main

import (
	"fmt"
	"os"
	"os/user"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
)

func audit() {
	zone := ctx.DefaultZone()
	for i, arg := range os.Args[1:] {
		if arg == "-z" {
			zone = os.Args[i+1]
			break
		}
		if arg == "--generate-bash-completion" {
			return
		}
	}

	z := ctx.Zone(zone)
	if z.GkAuditCluster == "" || z.GkAuditTopic == "" {
		return
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	zkcluster := zkzone.NewCluster(z.GkAuditCluster)
	cf := sarama.NewConfig()
	cf.Net.DialTimeout = time.Second * 4
	p, err := sarama.NewSyncProducer(zkcluster.BrokerList(), cf)
	if err != nil {
		// silently drop the err
		return
	}
	defer p.Close()

	ip, err := ctx.LocalIP()
	if err != nil {
		panic(err)
	}
	u, _ := user.Current()
	msg := fmt.Sprintf("[%s@%s] (%s) %+v", u.Name, ip.String(), gafka.BuildId, os.Args[1:])
	p.SendMessage(&sarama.ProducerMessage{
		Topic: z.GkAuditTopic,
		Value: sarama.StringEncoder(msg),
	})

}
