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

type Leader struct {
	Ui  cli.Ui
	Cmd string

	zkcluster *zk.ZkCluster
	zone      string
	cluster   string
	topic     string
	partition string
}

func (this *Leader) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("leader", flag.ContinueOnError)
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

func (this *Leader) generateReassignFile() string {
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

func (this *Leader) executeReassignment() {
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

func (*Leader) Synopsis() string {
	return "Restore the leadership balance for a given topic partition"
}

func (this *Leader) Help() string {
	help := fmt.Sprintf(`
Usage: %s leader -z zone -c cluster [options]

    Restore the leadership balance for a given topic partition.   

    e,g.
      gk leader -z prod -c trade -t order -p 0,1

Options:

    -t topic

    -p partitionId
      Multiple partition ids seperated by comma.
      e,g. -p 0,1

`, this.Cmd)
	return strings.TrimSpace(help)
}
