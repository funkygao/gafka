// a script to test kafka consumer group.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/kafka-cg/consumergroup"
)

var (
	zk     string
	chroot string
	topic  string
)

func init() {
	flag.StringVar(&zk, "zk", "localhost:2181", "zk addr")
	flag.StringVar(&chroot, "p", "", "zk chroot path")
	flag.StringVar(&topic, "t", "", "kafka topic name")
	flag.Parse()

	if topic == "" {
		panic("topic must be provided")
	}

	sarama.Logger = log.New(os.Stdout, color.Green("[Sarama]"),
		log.LstdFlags|log.Lshortfile)
}

func main() {
	cf := consumergroup.NewConfig()
	cf.Net.DialTimeout = time.Second * 10
	cf.Net.WriteTimeout = time.Second * 10
	cf.Net.ReadTimeout = time.Second * 10
	cf.ChannelBufferSize = 0
	cf.Consumer.Return.Errors = true
	cf.Zookeeper.Chroot = chroot
	cf.Consumer.MaxProcessingTime = 100 * time.Millisecond // chan recv timeout
	cf.Zookeeper.Timeout = time.Second
	cf.Offsets.CommitInterval = time.Minute
	cf.Offsets.ProcessingTimeout = time.Second
	cf.Offsets.Initial = sarama.OffsetOldest
	cg, err := consumergroup.JoinConsumerGroup("group1", []string{topic},
		[]string{zk}, cf)
	if err != nil {
		panic(err)
	}

	fmt.Println("consumer group started")
	var i int
	for msg := range cg.Messages() {
		fmt.Println(i, string(msg.Value))
		i++

		cg.CommitUpto(msg)
	}

}
