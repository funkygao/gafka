package controller

import (
	"sort"

	"github.com/funkygao/gafka/zk"
)

func assignJobsToActors(actors zk.ActorList, jobs zk.JobQueueList) (decision map[string]zk.JobQueueList) {
	decision = make(map[string]zk.JobQueueList)

	jLen, aLen := len(jobs), len(actors)
	if aLen == 0 {
		return
	}

	sort.Sort(jobs)
	sort.Sort(actors)

	nJobsPerActor, nActorsWithExtraJob := jLen/aLen, jLen%aLen

	for myActorPosition := 0; myActorPosition < aLen; myActorPosition++ {
		extraN := 1
		if myActorPosition+1 > nActorsWithExtraJob {
			extraN = 0
		}
		nJobs := nJobsPerActor + extraN
		startJob := nJobsPerActor*myActorPosition + min(myActorPosition, nActorsWithExtraJob)
		for jobIdx := startJob; jobIdx < startJob+nJobs; jobIdx++ {
			if _, present := decision[actors[myActorPosition]]; !present {
				decision[actors[myActorPosition]] = make(zk.JobQueueList, 0)
			}
			decision[actors[myActorPosition]] = append(decision[actors[myActorPosition]], jobs[jobIdx])
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
