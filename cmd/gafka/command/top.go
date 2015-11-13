package command

import (
	"fmt"
	//"sort"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/sarama"
)

type Top struct {
	Ui       cli.Ui
	mu       sync.Mutex
	counters map[string]int64 // key is cluster:topic
}

func (this *Top) Run(args []string) (exitCode int) {
	if len(args) == 0 {
		this.Ui.Error("zone required")
		this.Ui.Output(this.Help())
		return 2
	}

	this.counters = make(map[string]int64)

	for _, zone := range args {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, config.ZonePath(zone)))
		zkzone.WithinClusters(func(cluster string, path string) {
			zkcluster := zkzone.NewCluster(cluster)
			go this.clusterTop(zkcluster)
		})
	}

	for {
		select {
		case <-time.After(time.Second * 5):

			c := exec.Command("clear")
			c.Stdout = os.Stdout
			c.Run()

			// header
			this.Ui.Output(fmt.Sprintf("%30s %50s %15s", "cluster", "topic", "num"))
			this.Ui.Output(fmt.Sprintf(strings.Repeat("-", 97)))

			this.showAndResetCounters()
		}
	}

	return

}

func (this *Top) showAndResetCounters() {
	this.mu.Lock()
	defer this.mu.Unlock()

	// sort by cluster name
	for ct, num := range this.counters {
		p := strings.SplitN(ct, ":", 2)
		this.Ui.Output(fmt.Sprintf("%30s %50s %15d", p[0], p[1], num))
	}

	this.counters = make(map[string]int64)
}

func (this *Top) clusterTop(zkcluster *zk.ZkCluster) {
	cluster := zkcluster.Name()
	brokerList := zkcluster.BrokerList()
	if len(brokerList) == 0 {
		return
	}

	kfkClient, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		return
	}
	defer kfkClient.Close()

	for {
		topics, err := kfkClient.Topics()
		if err != nil || len(topics) == 0 {
			return
		}

		for _, topic := range topics {
			msgs := int64(0)
			alivePartitions, err := kfkClient.WritablePartitions(topic)
			if err != nil {
				panic(err)
			}

			for _, partitionID := range alivePartitions {
				latestOffset, err := kfkClient.GetOffset(topic, partitionID,
					sarama.OffsetNewest)
				if err != nil {
					panic(err)
				}

				msgs += latestOffset
			}

			this.mu.Lock()
			this.counters[cluster+":"+topic] += msgs
			this.mu.Unlock()
		}

		time.Sleep(time.Second)
		kfkClient.RefreshMetadata(topics...)
	}

}

func (*Top) Synopsis() string {
	return "Display top kafka cluster activities"
}

func (*Top) Help() string {
	help := `
Usage: gafka top [zone ...]

	Display top kafka cluster activities
`
	return strings.TrimSpace(help)
}
