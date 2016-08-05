package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gocli"
)

type Rebalance struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Rebalance) Run(args []string) (exitCode int) {
	var (
		curRebalances int
		curConsumers  int
	)
	cmdFlags := flag.NewFlagSet("partition", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.IntVar(&curRebalances, "p", 17, "partitions count")
	cmdFlags.IntVar(&curConsumers, "c", 4, "consumers count")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	fmt.Printf("The topic has %d partitions, and consumer group has %d instances\n",
		curRebalances, curConsumers)

	nPartsPerConsumer := curRebalances / curConsumers
	nConsumersWithExtraPart := curRebalances % curConsumers

	partitionOwnershipDecision := make(map[int][]int)

	for myConsumerPosition := 0; myConsumerPosition < curConsumers; myConsumerPosition++ {
		x := 1
		if myConsumerPosition+1 > nConsumersWithExtraPart {
			x = 0
		}
		nParts := nPartsPerConsumer + x

		startPart := nPartsPerConsumer*myConsumerPosition + min(myConsumerPosition, nConsumersWithExtraPart)
		for pid := startPart; pid < startPart+nParts; pid++ {
			if _, present := partitionOwnershipDecision[myConsumerPosition]; !present {
				partitionOwnershipDecision[myConsumerPosition] = []int{pid}
			} else {
				partitionOwnershipDecision[myConsumerPosition] = append(partitionOwnershipDecision[myConsumerPosition], pid)
			}
		}
	}

	sortedConsumerIds := make([]int, 0, len(partitionOwnershipDecision))
	for cid, _ := range partitionOwnershipDecision {
		sortedConsumerIds = append(sortedConsumerIds, cid)
	}
	sort.Ints(sortedConsumerIds)
	for _, cid := range sortedConsumerIds {
		fmt.Printf("consumer[%d] got %2d: %+v\n", cid,
			len(partitionOwnershipDecision[cid]),
			partitionOwnershipDecision[cid])
	}

	return
}

func (*Rebalance) Synopsis() string {
	return "The reblance algorithm of kafka consumer group"
}

func (this *Rebalance) Help() string {
	help := fmt.Sprintf(`
Usage: %s partition [options]

    The reblance algorithm of kafka consumer group

    -p partitions

    -c consumer instances

`, this.Cmd)
	return strings.TrimSpace(help)
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
