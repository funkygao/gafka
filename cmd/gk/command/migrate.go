package command

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/pipestream"
	"github.com/funkygao/golib/rand"
)

const (
	reassignNodeFilename = "reassignment-node.json"
)

type Migrate struct {
	Ui  cli.Ui
	Cmd string

	zkcluster  *zk.ZkCluster
	zone       string
	cluster    string
	topic      string
	brokerId   string
	partition  string
	verifyMode bool
}

func (this *Migrate) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("migrate", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.topic, "t", "", "")
	cmdFlags.StringVar(&this.partition, "p", "", "")
	cmdFlags.StringVar(&this.brokerId, "brokers", "", "")
	cmdFlags.BoolVar(&this.verifyMode, "verify", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z", "-c", "-t").
		invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	this.zkcluster = zkzone.NewCluster(this.cluster)

	if this.verifyMode {
		this.Ui.Info(fmt.Sprintf("You MUST manually remove the %s after migration is done.", reassignNodeFilename))
		this.Ui.Info(fmt.Sprintf("After verify ok, modify producer/consumer to point to new brokers!"))

		for {
			this.Ui.Output(fmt.Sprintf("%s", time.Now().String()))
			this.verify()

			time.Sleep(time.Second * 5)
		}

		return
	}

	if validateArgs(this, this.Ui).
		require("-z", "-c", "-t", "-p", "-brokers").
		requireAdminRights("-z").
		invalid(args) {
		return 2
	}

	//this.ensureBrokersAreAlive()
	data := this.generateReassignFile()
	this.Ui.Output(data)
	yes, _ := this.Ui.Ask("Are you sure to execute the migration? [Y/N]")
	if yes == "Y" {
		this.executeReassignment()
	} else {
		this.Ui.Output("bye")
	}

	return
}

func (this *Migrate) ensureBrokersAreAlive() {
	brokerIds := strings.Split(this.brokerId, ",")
	liveBrokers := this.zkcluster.Brokers()
	for _, id := range brokerIds {
		if _, present := liveBrokers[id]; !present {
			this.Ui.Error(fmt.Sprintf("broker:%s not alive", id))
			os.Exit(1)
		}
	}

}

func (this *Migrate) normalizePartitions() {
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

// generate reassignment-node.json
func (this *Migrate) generateReassignFile() string {
	// {"version":1,"partitions":[{"topic":"fortest1","partition":0,"replicas":[3,4]}

	type PartitionMeta struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Replicas  []int  `json:"replicas"`
	}
	type ReassignMeta struct {
		Version    int             `json:"version"`
		Partitions []PartitionMeta `json:"partitions"`
	}

	var js ReassignMeta
	js.Version = 1
	js.Partitions = make([]PartitionMeta, 0)
	this.normalizePartitions()
	var topics []string
	if this.topic == "_ALL_" {
		var err error
		topics, err = this.zkcluster.Topics()
		swallow(err)
	} else {
		topics = []string{this.topic}
	}
	for _, topic := range topics {
		for _, p := range strings.Split(this.partition, ",") {
			p = strings.TrimSpace(p)
			pid, err := strconv.Atoi(p)
			swallow(err)

			pmeta := PartitionMeta{
				Topic:     topic,
				Partition: pid,
				Replicas:  make([]int, 0),
			}
			for _, b := range strings.Split(this.brokerId, ",") {
				b = strings.TrimSpace(b)
				bid, err := strconv.Atoi(b)
				swallow(err)

				pmeta.Replicas = append(pmeta.Replicas, bid)

				// shuffle for load balance the preferred leader
				pmeta.Replicas = rand.ShuffleInts(pmeta.Replicas)
			}

			js.Partitions = append(js.Partitions, pmeta)
		}
	}

	b, err := json.Marshal(js)
	swallow(err)
	swallow(ioutil.WriteFile(reassignNodeFilename, b, 0644))
	return string(b)
}

func (this *Migrate) executeReassignment() {
	/*
		1. kafka-reassign-partitions.sh write /admin/reassign_partitions
		2. controller listens to the path above
		3. For each topic partition, the controller does the following:
		  3.1. Start new replicas in RAR – AR (RAR = Reassigned Replicas, AR = original list of Assigned Replicas)
		  3.2. Wait until new replicas are in sync with the leader
		  3.3. If the leader is not in RAR, elect a new leader from RAR
		  3.4 4. Stop old replicas AR – RAR
		  3.5. Write new AR
		  3.6. Remove partition from the /admin/reassign_partitions path

	*/
	cmd := pipestream.New(fmt.Sprintf("%s/bin/kafka-reassign-partitions.sh", ctx.KafkaHome()),
		fmt.Sprintf("--zookeeper %s", this.zkcluster.ZkConnectAddr()),
		fmt.Sprintf("--reassignment-json-file %s", reassignNodeFilename),
		fmt.Sprintf("--execute"),
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

func (this *Migrate) verify() {
	cmd := pipestream.New(fmt.Sprintf("%s/bin/kafka-reassign-partitions.sh", ctx.KafkaHome()),
		fmt.Sprintf("--zookeeper %s", this.zkcluster.ZkConnectAddr()),
		fmt.Sprintf("--reassignment-json-file %s", reassignNodeFilename),
		fmt.Sprintf("--verify"),
	)
	err := cmd.Open()
	if err != nil {
		return
	}
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "successfully") {
			this.Ui.Info(scanner.Text())
		} else {
			this.Ui.Warn(scanner.Text())
		}
	}

}

func (*Migrate) Synopsis() string {
	return "Migrate given topic partition to specified broker ids"
}

func (this *Migrate) Help() string {
	help := fmt.Sprintf(`
Usage: %s migrate -z zone -c cluster [options]

    %s

    e,g. migrate partition 0 of order to broker 2 as master and 3 as replica, then verify
      gk migrate -z prod -c trade -t order -p 0 -brokers 2,3
      gk migrate -z prod -c trade -t order -p 0 -brokers 2,3 -verify

Options:

    -t topic
      If '-t _ALL_', will migrate all topics within a cluster. 
      You must know what that means before proceeding.

    -p partitionId   
      Multiple partition ids seperated by comma or -.
      e,g. -p 0,1
      e,g. -p 2-20

    -brokers id1,id2,idN
      Migrate the topic to given broker ids.
      brokers order is IMPORTANT! The 1st is the preferred leader.

    -verify
      Verify the migration ongoing.      

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
