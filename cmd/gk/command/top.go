package command

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/sarama"
)

const (
	topInterval     = 5
	topIntervalTime = time.Second * topInterval
)

type Top struct {
	Ui  cli.Ui
	Cmd string

	mu           sync.Mutex
	limit        int
	topic        string
	counters     map[string]int // key is cluster:topic TODO int64
	lastCounters map[string]int
}

func (this *Top) Run(args []string) (exitCode int) {
	var zone string
	cmdFlags := flag.NewFlagSet("top", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&this.topic, "t", "", "")
	cmdFlags.IntVar(&this.limit, "n", 35, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).require("-z").invalid(args) {
		return 2
	}

	this.counters = make(map[string]int)
	this.lastCounters = make(map[string]int)

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, config.ZonePath(zone)))
	zkzone.WithinClusters(func(cluster string, path string) {
		zkcluster := zkzone.NewCluster(cluster)
		go this.clusterTop(zkcluster)
	})

	for {
		select {
		case <-time.After(topIntervalTime):
			c := exec.Command("clear")
			c.Stdout = os.Stdout
			c.Run()

			// header
			this.Ui.Output(fmt.Sprintf("%30s %50s %20s %10s",
				"cluster", "topic", "num", "mps"))
			this.Ui.Output(fmt.Sprintf(strings.Repeat("-", 113)))

			this.showAndResetCounters()
		}
	}

	return

}

func (this *Top) showAndResetCounters() {
	this.mu.Lock()
	defer this.mu.Unlock()

	counterFlip := make(map[int]string)
	sortedNum := make([]int, 0, len(this.counters))
	for ct, num := range this.counters {
		if this.topic != "" && !strings.HasSuffix(ct, ":"+this.topic) {
			continue
		}

		counterFlip[num] = ct
		if num > 100 { // TODO kill the magic number
			sortedNum = append(sortedNum, num)
		}
	}
	sort.Ints(sortedNum)

	for i := len(sortedNum) - 1; i >= 0; i-- {
		if len(sortedNum)-i > this.limit {
			break
		}

		num := sortedNum[i]
		p := strings.SplitN(counterFlip[num], ":", 2)
		mps := (num - this.lastCounters[counterFlip[num]]) / topInterval // msg per sec
		this.Ui.Output(fmt.Sprintf("%30s %50s %20s %10s", p[0], p[1],
			gofmt.Comma(int64(num)), gofmt.Comma(int64(mps))))
	}

	// record last counters and reset current counters
	for k, v := range this.counters {
		this.lastCounters[k] = v
	}
	this.counters = make(map[string]int)
}

func (this *Top) clusterTop(zkcluster *zk.ZkCluster) {
	cluster := zkcluster.Name()
	brokerList := zkcluster.BrokerList()
	if len(brokerList) == 0 {
		return
	}

	kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		return
	}
	defer kfk.Close()

	for {
		topics, err := kfk.Topics()
		if err != nil || len(topics) == 0 {
			return
		}

		for _, topic := range topics {
			if this.topic != "" && this.topic != topic {
				continue
			}

			msgs := int64(0)
			alivePartitions, err := kfk.WritablePartitions(topic)
			if err != nil {
				panic(err)
			}

			for _, partitionID := range alivePartitions {
				latestOffset, err := kfk.GetOffset(topic, partitionID,
					sarama.OffsetNewest)
				if err != nil {
					panic(err)
				}

				msgs += latestOffset
			}

			this.mu.Lock()
			this.counters[cluster+":"+topic] = int(msgs)
			this.mu.Unlock()
		}

		time.Sleep(time.Second)
		kfk.RefreshMetadata(topics...)
	}

}

func (*Top) Synopsis() string {
	return "Display top kafka cluster activities"
}

func (this *Top) Help() string {
	help := fmt.Sprintf(`
Usage: %s top [options]

	Display top kafka cluster activities

  -z zone

  -t topic

  -n limit
`, this.Cmd)
	return strings.TrimSpace(help)
}
