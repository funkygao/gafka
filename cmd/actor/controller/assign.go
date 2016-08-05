package controller

import (
	"sort"

	"github.com/funkygao/gafka/zk"
)

func assignJobsToActors(actors zk.ActorList, jobQueues zk.JobQueueList) (decision map[string]zk.JobQueueList) {
	decision = make(map[string]zk.JobQueueList)

	jLen, aLen := len(jobQueues), len(actors)
	if aLen == 0 || jLen == 0 {
		return
	}

	sort.Sort(jobQueues)
	sort.Sort(actors)

	nJobQueuesPerActor, nActorsWithExtraJobQueue := jLen/aLen, jLen%aLen

	for currentActorIdx := 0; currentActorIdx < aLen; currentActorIdx++ {
		extraN := 1
		if currentActorIdx+1 > nActorsWithExtraJobQueue {
			extraN = 0
		}

		nJobQueues := nJobQueuesPerActor + extraN
		startJobQueueIdx := nJobQueuesPerActor*currentActorIdx + min(currentActorIdx, nActorsWithExtraJobQueue)
		for j := startJobQueueIdx; j < startJobQueueIdx+nJobQueues; j++ {
			if _, present := decision[actors[currentActorIdx]]; !present {
				decision[actors[currentActorIdx]] = make(zk.JobQueueList, 0)
			}
			decision[actors[currentActorIdx]] = append(decision[actors[currentActorIdx]], jobQueues[j])
		}
	}
	return
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
