package command

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/golib/pipestream"
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
		require("-z", "-c").
		invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	this.zkcluster = zkzone.NewCluster(this.cluster)

	if this.verifyMode {
		this.Ui.Info(fmt.Sprintf("You MUST manually remove the %s after migration is done.", reassignNodeFilename))
		this.Ui.Info(fmt.Sprintf("After verify ok, modify producer/consumer to point to new brokers!"))

		for {
			this.verify()

			time.Sleep(time.Second * 2)
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

// generate reassignment-node.json
func (this *Migrate) generateReassignFile() string {
	// {"version":1,"partitions":[{"topic":"fortest1","partition":0,"replicas":[3,4]}
	data := strings.TrimSpace(fmt.Sprintf(`
{"version":1, "partitions":[{"topic":"%s","partition":%s,"replicas":[%s]}]}
		`, this.topic, this.partition, this.brokerId))
	swallow(ioutil.WriteFile(reassignNodeFilename, []byte(data), 0644))
	return data
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
		this.Ui.Output(color.Yellow(scanner.Text()))
	}

}

func (*Migrate) Synopsis() string {
	return "Migrate given topic partition to specified broker ids"
}

func (this *Migrate) Help() string {
	help := fmt.Sprintf(`
Usage: %s migrate -z zone -c cluster [options]

    Migrate given topic partition to specified broker ids.    

Options:

    -t topic

    -p partitionId      

    -brokers id1,id2,idN
      Migrate the topic to given broker ids.

    -verify
      Verify the migration ongoing.      

`, this.Cmd)
	return strings.TrimSpace(help)
}
