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
	"github.com/funkygao/termui"
	"github.com/nsf/termbox-go"
)

type Top struct {
	Ui  cli.Ui
	Cmd string

	zone           string
	clusterPattern string

	mu sync.Mutex

	round int

	showProgressBar bool
	who             string
	limit           int
	topInterval     int
	batchMode       bool
	dashboardGraph  bool
	topicPattern    string

	counters         map[string]float64 // key is cluster:topic
	lastCounters     map[string]float64
	consumerCounters map[string]float64

	totalConsumerMps []float64
	totalMps         []float64 // for the dashboard graph
	maxMps           float64
}

func (this *Top) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("top", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.topicPattern, "t", "", "")
	cmdFlags.IntVar(&this.topInterval, "interval", 5, "refresh interval")
	cmdFlags.StringVar(&this.clusterPattern, "c", "", "")
	cmdFlags.IntVar(&this.limit, "n", 33, "")
	cmdFlags.StringVar(&this.who, "who", "producer", "")
	cmdFlags.BoolVar(&this.showProgressBar, "bar", false, "")
	cmdFlags.BoolVar(&this.dashboardGraph, "d", false, "")
	cmdFlags.BoolVar(&this.batchMode, "b", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z").
		invalid(args) {
		return 2
	}

	if this.dashboardGraph {
		this.topInterval = 20
		this.who = "both"
		go this.clusterOffsetSummary()
	}

	if this.who == "c" || this.who == "consumer" {
		if this.topInterval < 20 {
			this.topInterval = 20 // consumer groups only refresh offset per minute
		}

	}

	this.counters = make(map[string]float64)
	this.lastCounters = make(map[string]float64)
	this.consumerCounters = make(map[string]float64)
	this.totalMps = make([]float64, 0, 1000)
	this.totalConsumerMps = make([]float64, 0, 1000)

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if !patternMatched(zkcluster.Name(), this.clusterPattern) {
			return
		}

		switch this.who {
		case "p", "producer":
			go this.clusterTopProducers(zkcluster)

		case "c", "consumer":
			go this.clusterTopConsumers(zkcluster)

		case "both":
			go this.clusterTopConsumers(zkcluster)
			go this.clusterTopProducers(zkcluster)

		default:
			this.Ui.Error(fmt.Sprintf("unknown type: %s", this.who))
		}
	})

	if this.dashboardGraph {
		this.drawDashboard()
		return
	}

	bar := progress.New(this.topInterval)
	for {
		if this.batchMode {
			this.Ui.Output(bjtime.TimeToString(bjtime.NowBj()))
		} else {
			refreshScreen()
		}

		// header
		this.Ui.Output(fmt.Sprintf("%-9s %20s %50s %20s %15s",
			this.who, "cluster", "topic", "num", "mps")) // mps=msg per second
		this.Ui.Output(fmt.Sprintf(strings.Repeat("-", 118)))

		this.showAndResetCounters()

		if !this.batchMode {
			this.showRefreshBar(bar)
		} else {
			time.Sleep(time.Duration(this.topInterval) * time.Second)
		}

	}

	return
}

func (this *Top) drawDashboard() {
	err := termui.Init()
	if err != nil {
		panic(err)
	}
	defer termui.Close()

	termui.UseTheme("helloworld")

	refreshProducerData := func() []float64 {
		this.showAndResetCounters()
		return this.totalMps
	}

	refreshConsumerData := func() []float64 {
		return this.totalConsumerMps
	}

	producerChart := termui.NewLineChart()
	producerChart.Mode = "dot"
	producerChart.Border.Label = fmt.Sprintf("producer mps totals: %s %s %s",
		this.zone, this.clusterPattern, this.topicPattern)
	producerChart.Data = refreshProducerData()
	producerChart.Width = termui.TermWidth() / 2
	producerChart.Height = termui.TermHeight()
	producerChart.X = 0
	producerChart.Y = 0
	producerChart.AxesColor = termui.ColorWhite
	producerChart.LineColor = termui.ColorGreen | termui.AttrBold

	consumerChart := termui.NewLineChart()
	consumerChart.Mode = "dot"
	consumerChart.Border.Label = fmt.Sprintf("consumer mps totals: %s %s %s",
		this.zone, this.clusterPattern, this.topicPattern)
	consumerChart.Data = refreshConsumerData()
	consumerChart.Width = termui.TermWidth() / 2
	consumerChart.Height = termui.TermHeight()
	consumerChart.X = termui.TermWidth() / 2
	consumerChart.Y = 0
	consumerChart.AxesColor = termui.ColorWhite
	consumerChart.LineColor = termui.ColorRed | termui.AttrBold

	evt := make(chan termbox.Event)
	go func() {
		for {
			evt <- termbox.PollEvent()
		}
	}()

	termui.Render(producerChart, consumerChart)
	tick := time.NewTicker(time.Duration(this.topInterval) * time.Second)
	defer tick.Stop()
	rounds := 0
	for {
		select {
		case e := <-evt:
			if e.Type == termbox.EventKey && e.Ch == 'q' {
				return
			}

		case <-tick.C:
			// refresh data, and skip the first 2 rounds
			rounds++
			if rounds > 1 {
				producerChart.Data = refreshProducerData()
				consumerChart.Data = refreshConsumerData()
				termui.Render(producerChart, consumerChart)
			}

		}
	}
}

func (this *Top) showRefreshBar(bar *progress.Progress) {
	this.Ui.Output("")
	for i := 1; i <= this.topInterval; i++ {
		if this.showProgressBar {
			bar.ShowProgress(i)
		}

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
		if !limitReached && this.limit > 0 && len(sortedNum)-i > this.limit {
			limitReached = true
		}

		num := sortedNum[i]
		mps := float64(num-this.lastCounters[counterFlip[num]]) / float64(this.topInterval) // msg per sec
		if this.round > 1 {
			totalNum += num
			totalMps += mps
		}

		if limitReached {
			othersNum += num
			othersMps += mps
		} else if !this.dashboardGraph {
			clusterAndTopic := strings.SplitN(counterFlip[num], ":", 2)
			this.Ui.Output(fmt.Sprintf("%30s %50s %20s %15.2f",
				clusterAndTopic[0], clusterAndTopic[1],
				gofmt.Comma(int64(num)),
				mps))
		}
	}

	// display the summary footer
	this.round++
	if this.dashboardGraph {
		if len(this.totalMps) > 5000 {
			// too long, so reset
			this.totalMps = make([]float64, 0, 1000)
		}

		this.totalMps = append(this.totalMps, totalMps)
	} else {
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

		// max
		if this.maxMps < totalMps {
			this.maxMps = totalMps
		}
		this.Ui.Output(fmt.Sprintf("%30s %50s %20s %15.2f",
			"--MAX--", "--MAX--",
			"-",
			this.maxMps))
	}

	// record last counters and reset current counters
	for k, v := range this.counters {
		this.lastCounters[k] = v
	}
	this.counters = make(map[string]float64)
}

func (this *Top) clusterOffsetSummary() {
	var lastOffsets float64
	var total float64
	for {
		total = 0.
		this.mu.Lock()
		for _, n := range this.consumerCounters {
			total += n
		}
		this.mu.Unlock()

		if lastOffsets > 1 {
			this.totalConsumerMps = append(this.totalConsumerMps,
				(float64(total)-lastOffsets)/float64(this.topInterval))
		}

		lastOffsets = float64(total)

		time.Sleep(time.Second * time.Duration(this.topInterval))
	}
}

func (this *Top) clusterTopConsumers(zkcluster *zk.ZkCluster) {
	var topic string
	for {
		total := zkcluster.TotalConsumerOffsets(this.topicPattern)
		if this.topicPattern != "" {
			topic = this.topicPattern
		} else {
			topic = "-all-"
		}

		key := zkcluster.Name() + ":" + topic

		this.mu.Lock()
		this.consumerCounters[key] = float64(total)
		this.mu.Unlock()

		if !this.dashboardGraph {
			this.mu.Lock()
			this.counters[key] = float64(total)
			this.mu.Unlock()
		}

		time.Sleep(time.Second * time.Duration(this.topInterval))
	}

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
	return "Unix “top” like utility for kafka"
}

func (this *Top) Help() string {
	help := fmt.Sprintf(`
Usage: %s top [options]

    Unix “top” like utility for kafka

Options:

    -z zone

    -c cluster pattern

    -t topic pattern    

    -interval interval
      Refresh interval in seconds.

    -n limit

    -d
      Draw dashboard in graph.

    -bar
      Show progress bar.

    -b 
      Batch mode operation. 
      Could be useful for sending output from top to other programs or to a file.

    -who <%s%s|%s%s>
`, this.Cmd, color.Colorize([]string{color.Underscore}, "p"), "roducer",
		color.Colorize([]string{color.Underscore}, "c"), "onsumer")
	return strings.TrimSpace(help)
}
