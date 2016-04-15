package command

import (
	"flag"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/termui"
)

type TopBroker struct {
	Ui  cli.Ui
	Cmd string

	mu sync.Mutex

	zone, cluster, topic string
	drawMode             bool
	interval             time.Duration
	shortIp              bool

	offsets     map[string]int64 // host => offset sum
	lastOffsets map[string]int64
}

func (this *TopBroker) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("topbroker", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.topic, "t", "", "")
	cmdFlags.BoolVar(&this.drawMode, "d", false, "")
	cmdFlags.BoolVar(&this.shortIp, "shortip", false, "")
	cmdFlags.DurationVar(&this.interval, "i", time.Second*5, "refresh interval")

	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.offsets = make(map[string]int64)
	this.lastOffsets = make(map[string]int64)

	if this.interval.Seconds() < 1 {
		this.interval = time.Second
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if !patternMatched(zkcluster.Name(), this.cluster) {
			return
		}

		go this.clusterTopProducers(zkcluster)
	})

	if this.drawMode {
		this.drawDashboard()
		return
	}

	ticker := time.NewTicker(this.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			refreshScreen()
			this.showAndResetCounters(true)

		}
	}

	return
}

func (this *TopBroker) drawDashboard() {
	termui.Init()
	width := termui.TermWidth()
	height := termui.TermHeight()
	termui.Close()
	maxWidth := width - 23
	var maxQps float64
	for {
		refreshScreen()
		datas := this.showAndResetCounters(false)
		maxQps = .0
		for _, data := range datas {
			if data.qps > maxQps {
				maxQps = data.qps
			}
		}

		if maxQps < 1 {
			// draw empty lines
			for _, data := range datas {
				this.Ui.Output(fmt.Sprintf("%20s", data.host))
			}

			continue
		}

		for idx, data := range datas {
			if idx >= height-1 {
				break
			}

			if data.qps < 0 {
				data.qps = -data.qps // FIXME
			}

			w := int(data.qps*100/maxQps) * maxWidth / 100
			qps := fmt.Sprintf("%.1f", data.qps)
			bar := ""
			barColorLen := 0
			for i := 0; i < w-len(qps); i++ {
				bar += color.Green("|")
				barColorLen += 9 // color.Green will add extra 9 chars
			}
			for i := len(bar) - barColorLen; i < maxWidth-len(qps); i++ {
				bar += " "
			}
			bar += qps

			this.Ui.Output(fmt.Sprintf("%20s [%s]", data.host, bar))
		}

		time.Sleep(this.interval)
	}

}

type brokerQps struct {
	host string
	qps  float64
}

func (this *TopBroker) showAndResetCounters(show bool) []brokerQps {
	this.mu.Lock()
	defer this.mu.Unlock()

	d := this.interval.Seconds()
	sortedHost := make([]string, 0, len(this.offsets))
	for host, _ := range this.offsets {
		sortedHost = append(sortedHost, host)
	}
	sort.Strings(sortedHost)

	if show {
		this.Ui.Output(fmt.Sprintf("%20s %8s", "host", "mps"))
	}

	totalQps := .0
	r := make([]brokerQps, 0, len(sortedHost))
	for _, host := range sortedHost {
		offset := this.offsets[host]
		qps := float64(0)
		if lastOffset, present := this.lastOffsets[host]; present {
			qps = float64(offset-lastOffset) / d
		}

		r = append(r, brokerQps{host, qps})
		totalQps += qps

		if show {
			this.Ui.Output(fmt.Sprintf("%20s %10.1f", host, qps))
		}
	}

	if show {
		this.Ui.Output(fmt.Sprintf("%20s %10.1f", "-TOTAL-", totalQps))
	}

	for host, offset := range this.offsets {
		this.lastOffsets[host] = offset
	}
	this.offsets = make(map[string]int64)

	return r
}

func (this *TopBroker) clusterTopProducers(zkcluster *zk.ZkCluster) {
	kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		return
	}
	defer kfk.Close()

	for {
		topics, err := kfk.Topics()
		swallow(err)

		for _, topic := range topics {
			if !patternMatched(topic, this.topic) {
				continue
			}

			partions, err := kfk.WritablePartitions(topic)
			swallow(err)
			for _, partitionID := range partions {
				leader, err := kfk.Leader(topic, partitionID)
				swallow(err)

				latestOffset, err := kfk.GetOffset(topic, partitionID,
					sarama.OffsetNewest)
				if err != nil {
					panic(err)
				}

				host, _, err := net.SplitHostPort(leader.Addr())
				swallow(err)

				if this.shortIp {
					host = shortIp(host)
				}

				this.mu.Lock()
				if _, present := this.offsets[host]; !present {
					this.offsets[host] = 0
				}
				this.offsets[host] += latestOffset
				this.mu.Unlock()
			}
		}

		time.Sleep(time.Second)
		kfk.RefreshMetadata(topics...)
	}
}

func (*TopBroker) Synopsis() string {
	return "Unix “top” like utility for kafka brokers"
}

func (this *TopBroker) Help() string {
	help := fmt.Sprintf(`
Usage: %s topbroker [options]

    Unix “top” like utility for kafka brokers

Options:

    -z zone
      Default %s

    -c cluster pattern

    -t topic pattern  

    -i interval
      Refresh interval in seconds.
      e,g. 5s    

    -d
      Draw dashboard in graph.  

    -shortip

`, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
