package command

import (
	"flag"
	"fmt"
	"net"
	"sort"
	"strings"
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

	zone, cluster, topic string
	interval             time.Duration
	shortIp              bool

	offsets     map[string]int64 // host => offset sum
	lastOffsets map[string]int64

	hostOffsetCh chan map[string]int64
	signalsCh    map[string]chan struct{}
}

func (this *TopBroker) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("topbroker", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.topic, "t", "", "")
	cmdFlags.BoolVar(&this.shortIp, "shortip", false, "")
	cmdFlags.DurationVar(&this.interval, "i", time.Second*5, "refresh interval")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.signalsCh = make(map[string]chan struct{})
	this.hostOffsetCh = make(chan map[string]int64)

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

		this.signalsCh[zkcluster.Name()] = make(chan struct{})

		go this.clusterTopProducers(zkcluster)
	})

	this.drawDashboard()

	return
}

func (this *TopBroker) startAll() {
	for _, ch := range this.signalsCh {
		ch <- struct{}{}
	}
}

func (this *TopBroker) collectAll() {
	for _, _ = range this.signalsCh {
		offsets := <-this.hostOffsetCh
		for host, offsetSum := range offsets {
			this.offsets[host] += offsetSum
		}
	}
}

func (this *TopBroker) drawDashboard() {
	termui.Init()
	width := termui.TermWidth()
	height := termui.TermHeight()
	termui.Close()
	maxWidth := width - 23

	var totalMaxQps, totalMaxBrokerQps float64
	for {
		time.Sleep(this.interval)

		this.startAll()
		this.collectAll()

		datas, maxQps, totalQps := this.showAndResetCounters()
		if maxQps < 1 {
			// draw empty lines
			for _, data := range datas {
				this.Ui.Output(fmt.Sprintf("%20s", data.host))
			}

			continue
		}

		if maxQps > totalMaxBrokerQps {
			totalMaxBrokerQps = maxQps
		}
		if totalQps > totalMaxQps {
			totalMaxQps = totalQps
		}

		refreshScreen()

		for idx, data := range datas {
			if idx >= height-2 {
				break
			}

			if data.qps < 0 {
				panic("negative qps")
			}

			this.renderQpsRow(data.host, data.qps, maxQps, maxWidth)
		}

		this.Ui.Output(fmt.Sprintf("%20s brokers:%d total:%s cum max[broker:%.1f total:%.1f]",
			"-SUMMARY-",
			len(datas), color.Green("%.1f", totalQps), totalMaxBrokerQps, totalMaxQps))
	}

}

func (this *TopBroker) renderQpsRow(host string, qps, maxQps float64, maxWidth int) {
	w := int(qps*100/maxQps) * maxWidth / 100
	qpsStr := fmt.Sprintf("%.1f", qps)
	bar := ""
	barColorLen := 0
	for i := 0; i < w-len(qpsStr); i++ {
		bar += color.Green("|")
		barColorLen += 9 // color.Green will add extra 9 chars
	}
	for i := len(bar) - barColorLen; i < maxWidth-len(qpsStr); i++ {
		bar += " "
	}
	bar += qpsStr

	this.Ui.Output(fmt.Sprintf("%20s [%s]", host, bar))
}

type brokerQps struct {
	host string
	qps  float64
}

func (this *TopBroker) showAndResetCounters() ([]brokerQps, float64, float64) {
	sortedHost := make([]string, 0, len(this.offsets))
	for host, _ := range this.offsets {
		sortedHost = append(sortedHost, host)
	}
	sort.Strings(sortedHost)

	totalQps, maxHostQps := .0, .0
	r := make([]brokerQps, 0, len(sortedHost))
	for _, host := range sortedHost {
		offset := this.offsets[host]
		qps := float64(0)
		if lastOffset, present := this.lastOffsets[host]; present {
			qps = float64(offset-lastOffset) / this.interval.Seconds()
		} else {
			continue
		}

		r = append(r, brokerQps{host, qps})

		totalQps += qps
		if qps > maxHostQps {
			maxHostQps = qps
		}
	}

	for host, offset := range this.offsets {
		this.lastOffsets[host] = offset
	}
	this.offsets = make(map[string]int64) // reset

	return r, maxHostQps, totalQps
}

func (this *TopBroker) clusterTopProducers(zkcluster *zk.ZkCluster) {
	kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		return
	}
	defer kfk.Close()

	for {
		hostOffsets := make(map[string]int64)

		topics, err := kfk.Topics()
		swallow(err)

		<-this.signalsCh[zkcluster.Name()]

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
				swallow(err)

				host, _, err := net.SplitHostPort(leader.Addr())
				swallow(err)
				if this.shortIp {
					host = shortIp(host)
				}

				if _, present := hostOffsets[host]; !present {
					hostOffsets[host] = 0
				}
				hostOffsets[host] += latestOffset
			}
		}

		this.hostOffsetCh <- hostOffsets

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

    -shortip
	  Print ending portion of broker ip address.

`, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
