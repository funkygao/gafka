package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/bjtime"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/golib/progress"
)

const (
	topInterval = 5
)

type Top struct {
	Ui  cli.Ui
	Cmd string

	mu             sync.Mutex
	limit          int
	batchMode      bool
	topicPattern   string
	clusterPattern string

	counters     map[string]float64 // key is cluster:topic
	lastCounters map[string]float64
}

func (this *Top) Run(args []string) (exitCode int) {
	var (
		zone string
		who  string
	)
	cmdFlags := flag.NewFlagSet("top", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&this.topicPattern, "t", "", "")
	cmdFlags.StringVar(&this.clusterPattern, "c", "", "")
	cmdFlags.IntVar(&this.limit, "n", 34, "")
	cmdFlags.StringVar(&who, "who", "producer", "")
	cmdFlags.BoolVar(&this.batchMode, "b", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z").
		invalid(args) {
		return 2
	}

	this.counters = make(map[string]float64)
	this.lastCounters = make(map[string]float64)

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	zkzone.WithinClusters(func(zkcluster *zk.ZkCluster) {
		if !patternMatched(zkcluster.Name(), this.clusterPattern) {
			return
		}

		switch who {
		case "p", "producer":
			go this.clusterTopProducers(zkcluster)

		case "c", "consumer":
			go this.clusterTopConsumers(zkcluster)

		default:
			this.Ui.Error(fmt.Sprintf("unknown type: %s", who))
		}
	})

	bar := progress.New(topInterval)
	for {
		if this.batchMode {
			this.Ui.Output(bjtime.TimeToString(bjtime.NowBj()))
		} else {
			refreshScreen()
		}

		// header
		this.Ui.Output(fmt.Sprintf("%30s %50s %20s %15s",
			"cluster", "topic", "num", "mps")) // mps=msg per second
		this.Ui.Output(fmt.Sprintf(strings.Repeat("-", 118)))

		this.showAndResetCounters()

		if !this.batchMode {
			this.showRefreshBar(bar)
		} else {
			time.Sleep(topInterval * time.Second)
		}

	}

	return

}

func (this *Top) showRefreshBar(bar *progress.Progress) {
	this.Ui.Output("")
	for i := 1; i <= topInterval; i++ {
		bar.ShowProgress(i)
		time.Sleep(time.Second)
	}
}

func (this *Top) showAndResetCounters() {
	this.mu.Lock()
	defer this.mu.Unlock()

	// FIXME counterFlip should be map[int][]string
	counterFlip := make(map[float64]string)
	sortedNum := make([]float64, 0, len(this.counters))
	for ct, num := range this.counters {
		if this.topicPattern != "" && !strings.HasSuffix(ct, ":"+this.topicPattern) {
			continue
		}

		counterFlip[num] = ct
		if num > 100 { // TODO kill the magic number
			sortedNum = append(sortedNum, num)
		}
	}
	sort.Float64s(sortedNum)

	othersNum := 0.
	othersMps := 0.
	totalNum := 0.
	totalMps := 0.
	limitReached := false
	for i := len(sortedNum) - 1; i >= 0; i-- {
		if !limitReached && len(sortedNum)-i > this.limit {
			limitReached = true
		}

		num := sortedNum[i]
		mps := float64(num-this.lastCounters[counterFlip[num]]) / float64(topInterval) // msg per sec
		totalNum += num
		totalMps += mps
		if limitReached {
			othersNum += num
			othersMps += mps
		} else {
			clusterAndTopic := strings.SplitN(counterFlip[num], ":", 2)
			this.Ui.Output(fmt.Sprintf("%30s %50s %20s %15.2f",
				clusterAndTopic[0], clusterAndTopic[1],
				gofmt.Comma(int64(num)),
				mps))
		}
	}

	if limitReached {
		// the catchall row
		this.Ui.Output(fmt.Sprintf("%30s %50s %20s %15.2f",
			"-OTHERS-", "-OTHERS-",
			gofmt.Comma(int64(othersNum)),
			othersMps))

		// total row
		this.Ui.Output(fmt.Sprintf("%30s %50s %20s %15.2f",
			"--TOTAL--", "--TOTAL--",
			gofmt.Comma(int64(totalNum)),
			totalMps))
	}

	// record last counters and reset current counters
	for k, v := range this.counters {
		this.lastCounters[k] = v
	}
	this.counters = make(map[string]float64)
}

func (this *Top) clusterTopConsumers(zkcluster *zk.ZkCluster) {

}

func (this *Top) clusterTopProducers(zkcluster *zk.ZkCluster) {
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
			if !patternMatched(topic, this.topicPattern) {
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
			this.counters[cluster+":"+topic] = float64(msgs)
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

Options:

    -z zone

    -c cluster pattern

    -t topic pattern    

    -n limit

    -b 
      Batch mode operation. 
      Could be useful for sending output from top to other programs or to a file.

    -who <%s%s|%s%s>
`, this.Cmd, color.Colorize([]string{color.Underscore}, "p"), "roducer",
		color.Colorize([]string{color.Underscore}, "c"), "onsumer")
	return strings.TrimSpace(help)
}
