package controller

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/zk"
)

func TestAssignJobsToActors_Normal(t *testing.T) {
	jobs := zk.JobQueueList([]string{"a", "b", "c", "d", "e"})
	actors := zk.ActorList([]string{"1", "2"})

	decision := assignJobsToActors(actors, jobs)
	t.Logf("%+v", decision)
	assert.Equal(t, 3, len(decision["1"]))
	assert.Equal(t, 2, len(decision["2"]))
}

func TestAssignJobsToActors_EmptyJobQueues(t *testing.T) {
	jobs := zk.JobQueueList([]string{})
	actors := zk.ActorList([]string{"1", "2"})

	decision := assignJobsToActors(actors, jobs)
	t.Logf("%+v", decision)
}

func TestAssignJobsToActors_ActorMoreThanJobQueues(t *testing.T) {
	jobs := zk.JobQueueList([]string{"job1"})
	actors := zk.ActorList([]string{"1", "2"})

	decision := assignJobsToActors(actors, jobs)
	t.Logf("%+v", decision)
	assert.Equal(t, 0, len(decision["2"]))
	assert.Equal(t, 1, len(decision["1"]))
}
