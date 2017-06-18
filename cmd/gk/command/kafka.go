package command

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Kafka struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Kafka) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("kafka", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	if len(args) == 0 {
		this.Ui.Error("missing <host:port>")
		return 2
	}

	sarama.Logger = log.New(os.Stderr, color.Magenta("[sarama]"), log.LstdFlags|log.Lshortfile)

	broker := args[len(args)-1]
	kfk, err := sarama.NewClient([]string{broker}, saramaConfig())
	if err != nil {
		this.Ui.Error(err.Error())
		return
	}
	defer kfk.Close()

	this.Ui.Info("get topics")
	topics, err := kfk.Topics()
	swallow(err)
	if len(topics) == 0 {
		return
	}

	for _, topic := range topics {
		this.Ui.Infof("  get writable partitions of topic[%s]", topic)
		alivePartitions, err := kfk.WritablePartitions(topic)
		swallow(err)

		this.Ui.Infof("  get partitions of topic[%s]", topic)
		partions, err := kfk.Partitions(topic)
		swallow(err)
		if len(alivePartitions) != len(partions) {
			this.Ui.Errorf("  topic[%s] has %d readonly partitions", topic, len(partions)-len(alivePartitions))
		}

		for _, partitionID := range alivePartitions {
			this.Ui.Infof("    get replicas of %s#%d", topic, partitionID)
			_, err := kfk.Replicas(topic, partitionID)
			if err != nil {
				this.Ui.Errorf("      <-%s", err.Error())
			}
		}
	}

	return
}

func (*Kafka) Synopsis() string {
	return "Debug a kafka broker with kafka protocol"
}

func (this *Kafka) Help() string {
	help := fmt.Sprintf(`
Usage: %s kafka <host:port>

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
