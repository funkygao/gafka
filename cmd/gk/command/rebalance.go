package command

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/pipestream"
)

const (
	preferredReplicaJsonFile = "preferred-replica.json"
)

type Rebalance struct {
	Ui  cli.Ui
	Cmd string

	zkcluster *zk.ZkCluster
	zone      string
	cluster   string
	topic     string
	partition string
}

func (this *Rebalance) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("rebalance", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.topic, "t", "", "")
	cmdFlags.StringVar(&this.partition, "p", "", "comma seperated ids")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z", "-c", "-t", "-p").
		requireAdminRights("-t").
		invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	this.zkcluster = zkzone.NewCluster(this.cluster)

	data := this.generateReassignFile()
	this.Ui.Output(data)
	yes, _ := this.Ui.Ask("Are you sure to execute? [Y/N]")
	if yes == "Y" {
		this.executeReassignment()
	} else {
		this.Ui.Output("bye")
	}

	return
}

func (this *Rebalance) generateReassignFile() string {
	// {"partitions":[{"topic":"t1", "partition":1}]}

	type PartitionMeta struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
	}
	type ReassignMeta struct {
		Partitions []PartitionMeta `json:"partitions"`
	}

	var js ReassignMeta
	js.Partitions = make([]PartitionMeta, 0)
	this.normalizePartitions()
	for _, p := range strings.Split(this.partition, ",") {
		p = strings.TrimSpace(p)
		pid, err := strconv.Atoi(p)
		swallow(err)

		pmeta := PartitionMeta{
			Topic:     this.topic,
			Partition: pid,
		}

		js.Partitions = append(js.Partitions, pmeta)
	}

	b, err := json.Marshal(js)
	swallow(err)
	swallow(ioutil.WriteFile(preferredReplicaJsonFile, b, 0644))
	return string(b)
}

func (this *Rebalance) normalizePartitions() {
	if strings.Contains(this.partition, "-") {
		// e,g. 0-10
		parts := strings.Split(this.partition, "-")
		startId, err := strconv.Atoi(parts[0])
		swallow(err)
		endId, err := strconv.Atoi(parts[1])
		swallow(err)
		if startId >= endId {
			panic("invalid partition id")
		}

		partitions := make([]string, 0)
		for i := startId; i <= endId; i++ {
			partitions = append(partitions, strconv.Itoa(i))
		}

		this.partition = strings.Join(partitions, ",")
	}
}

func (this *Rebalance) executeReassignment() {
	cmd := pipestream.New(fmt.Sprintf("%s/bin/kafka-preferred-replica-election.sh", ctx.KafkaHome()),
		fmt.Sprintf("--zookeeper %s", this.zkcluster.ZkConnectAddr()),
		fmt.Sprintf("--path-to-json-file %s", preferredReplicaJsonFile),
	)
	err := cmd.Open()
	if err != nil {
		return
	}
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		this.Ui.Output(color.Yellow(scanner.Text()))
	}
}

func (*Rebalance) Synopsis() string {
	return "Restore the leadership balance for a given topic partition"
}

func (this *Rebalance) Help() string {
	help := fmt.Sprintf(`
Usage: %s rebalance -z zone -c cluster [options]

    %s  

    e,g.
      gk rebalance -z prod -c trade -t order -p 0,1

Options:

    -t topic

    -p partitionId
      Multiple partition ids seperated by comma or -.
      e,g. -p 0,1
      e,g. -p 0-19

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
