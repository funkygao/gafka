package controller

import (
	"sort"

	"github.com/funkygao/gafka/zk"
)

func assignResourcesToActors(actors zk.ActorList, resources zk.ResourceList) (decision map[string]zk.ResourceList) {
	decision = make(map[string]zk.ResourceList)

	rLen, aLen := len(resources), len(actors)
	if aLen == 0 || rLen == 0 {
		return
	}

	sort.Sort(resources)
	sort.Sort(actors)

	nResourcesPerActor, nActorsWithExtraResource := rLen/aLen, rLen%aLen

	for currentActorIdx := 0; currentActorIdx < aLen; currentActorIdx++ {
		extraN := 1
		if currentActorIdx+1 > nActorsWithExtraResource {
			extraN = 0
		}

		nResources := nResourcesPerActor + extraN
		startResourceIdx := nResourcesPerActor*currentActorIdx + min(currentActorIdx, nActorsWithExtraResource)
		for j := startResourceIdx; j < startResourceIdx+nResources; j++ {
			if _, present := decision[actors[currentActorIdx]]; !present {
				decision[actors[currentActorIdx]] = make(zk.ResourceList, 0)
			}
			decision[actors[currentActorIdx]] = append(decision[actors[currentActorIdx]], resources[j])
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
