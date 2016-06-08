package command

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/funkygao/gocli"
)

type Partition struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Partition) Run(args []string) (exitCode int) {
	var (
		brokerN       int
		partitionsN   int
		replicaFactor int
	)
	cmdFlags := flag.NewFlagSet("partition", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.IntVar(&partitionsN, "p", 9, "partitions count")
	cmdFlags.IntVar(&brokerN, "b", 4, "brokers count")
	cmdFlags.IntVar(&replicaFactor, "r", 3, "replication factor")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	fmt.Printf("Assign %d partition/%d replicas to %d brokers\n",
		partitionsN, replicaFactor, brokerN)

	// brokers sorted with broker id

	startIndex := rand.Intn(brokerN)
	currentPartitionId := 0
	nextReplicaShift := rand.Intn(brokerN)

	m := make(map[int][]int)

	for i := 0; i < partitionsN; i++ {
		if currentPartitionId > 0 && (currentPartitionId%brokerN == 0) {
			nextReplicaShift += 1
		}

		firstReplicaIndex := (currentPartitionId + startIndex) % brokerN
		replicaList := []int{firstReplicaIndex}
		for j := 0; j < replicaFactor-1; j++ {
			replicaList = append(replicaList, replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerN))
		}

		m[currentPartitionId] = replicaList
		currentPartitionId++
	}

	brokerPartitionMap := make(map[int][]int)
	for partition, brokers := range m {
		sort.Ints(brokers)
		fmt.Printf("partition %2d: %+v\n", partition, brokers)

		for _, brokerId := range brokers {
			if _, present := brokerPartitionMap[brokerId]; !present {
				brokerPartitionMap[brokerId] = make([]int, 0)
			}
			brokerPartitionMap[brokerId] = append(brokerPartitionMap[brokerId], partition)
		}
	}

	for brokerId, partitions := range brokerPartitionMap {
		sort.Ints(partitions)
		fmt.Printf("broker %2d: partitions %+v\n", brokerId, partitions)
	}

	return
}

func (*Partition) Synopsis() string {
	return "How kafka assign partitions to brokers"
}

func (this *Partition) Help() string {
	help := fmt.Sprintf(`
Usage: %s partition [options]

    How kafka assign partitions to brokers

    -p partitions

    -b brokers

    -r replication factor

`, this.Cmd)
	return strings.TrimSpace(help)
}

func replicaIndex(firstReplicaIndex int, secondReplicaShift int, replicaIndex int, nBrokers int) int {
	shift := 1 + (secondReplicaShift+replicaIndex)%(nBrokers-1)
	return (firstReplicaIndex + shift) % nBrokers
}
