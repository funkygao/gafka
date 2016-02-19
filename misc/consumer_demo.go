package main

import (
	l "log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/kafka-cg/consumergroup"
	log "github.com/funkygao/log4go"
)

func main() {
	l.SetFlags(l.LstdFlags | l.Llongfile)
	l.SetPrefix(color.Magenta("[log]"))

	sarama.PanicHandler = func(err interface{}) {
		log.Warn("sarama got panic: %v", err)
	}

	sarama.Logger = l.New(os.Stdout, color.Blue("[Sarama]"),
		l.LstdFlags|l.Lshortfile)

	ctx.LoadFromHome()

	zone := "prod"
	cluster := "gateway"
	group := "30.group1"
	topic := "30.test.v1"

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	zkcluster := zkzone.NewCluster(cluster)

	cf := consumergroup.NewConfig()
	cf.Zookeeper.Chroot = zkcluster.Chroot()
	cf.Consumer.Return.Errors = true
	cf.Offsets.CommitInterval = time.Minute

	cg, err := consumergroup.JoinConsumerGroup(group, []string{topic},
		strings.Split(zkzone.ZkAddrs(), ","), cf)
	msg := <-cg.Messages()
	log.Info("%s", string(msg.Value))
	log.Info("to commit")
	err = cg.CommitUpto(msg)
	log.Info("commit: %v", err)

	go func() {
		e := <-cg.Errors()
		if e != nil {
			log.Info("%+v", e)
		}
	}()

	log.Info("to close")
	err = cg.Close()
	log.Info("close: %v", err)
}
