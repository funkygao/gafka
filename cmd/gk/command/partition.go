package command

import (
	"bufio"
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/pipestream"
	log "github.com/funkygao/log4go"
)

type Partition struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Partition) Run(args []string) (exitCode int) {
	var (
		zone       string
		topic      string
		cluster    string
		partitions int
	)
	cmdFlags := flag.NewFlagSet("partition", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&topic, "t", "", "")
	cmdFlags.IntVar(&partitions, "n", 1, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).require("-z", "-c", "-t", "-n").invalid(args) {
		return 2
	}

	zkAddrs := config.ZonePath(zone)
	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, config.ZonePath(zone)))
	zkAddrs = zkAddrs + zkzone.ClusterPath(cluster)
	this.addPartition(zkAddrs, topic, partitions)
	return
}

func (this *Partition) addPartition(zkAddrs string, topic string, partitions int) error {
	log.Info("adding partitions to topic: %s", topic)

	cmd := pipestream.New(fmt.Sprintf("%s/bin/kafka-topics.sh", config.KafkaHome()),
		fmt.Sprintf("--zookeeper %s", zkAddrs),
		fmt.Sprintf("--alter"),
		fmt.Sprintf("--topic %s", topic),
		fmt.Sprintf("--partitions %d", partitions),
	)
	err := cmd.Open()
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	var line string
	for scanner.Scan() {
		line = scanner.Text()
		log.Info(line)
	}
	err = scanner.Err()
	if err != nil {
		return err
	}
	cmd.Close()

	log.Info("added partitions to topic: %s", topic)
	return nil
}

func (*Partition) Synopsis() string {
	return "Add partition num to a topic"
}

func (this *Partition) Help() string {
	help := fmt.Sprintf(`
Usage: %s partition -z zone -c cluster -t topic -n num

	Add partition num to a topic
`, this.Cmd)
	return strings.TrimSpace(help)
}
