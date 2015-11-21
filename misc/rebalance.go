// The reblance algorithm of kafka consumer group
package main

import (
	"flag"
	"fmt"
	"sort"
)

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func main() {
	var (
		curPartitions int
		curConsumers  int
	)
	flag.IntVar(&curPartitions, "p", 17, "partitions count")
	flag.IntVar(&curConsumers, "c", 4, "consumers count")
	flag.Parse()

	fmt.Printf("The topic has %d partitions, and consumer group has %d threads\n",
		curPartitions, curConsumers)

	nPartsPerConsumer := curPartitions / curConsumers
	nConsumersWithExtraPart := curPartitions % curConsumers

	partitionOwnershipDecision := make(map[int][]int)

	for myConsumerPosition := 0; myConsumerPosition < curConsumers; myConsumerPosition++ {
		startPart := nPartsPerConsumer*myConsumerPosition + min(myConsumerPosition, nConsumersWithExtraPart)
		x := 1
		if myConsumerPosition+1 > nConsumersWithExtraPart {
			x = 0
		}
		nParts := nPartsPerConsumer + x

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
		fmt.Printf("thread[%d] got %2d: %+v\n", cid,
			len(partitionOwnershipDecision[cid]),
			partitionOwnershipDecision[cid])
	}

}
