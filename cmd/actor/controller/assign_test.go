package controller

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/zk"
)

func TestAssignJobsToActors(t *testing.T) {
	jobs := zk.JobList([]string{"a", "b", "c", "d", "e"})
	actors := zk.ActorList([]string{"1", "2"})

	decision := assignJobsToActors(actors, jobs)
	t.Logf("%+v", decision)
	assert.Equal(t, 3, len(decision["1"]))
	assert.Equal(t, 2, len(decision["2"]))
}

func TestAssignJobsToActorsEmpty(t *testing.T) {
	jobs := zk.JobList([]string{})
	actors := zk.ActorList([]string{"1", "2"})

	decision := assignJobsToActors(actors, jobs)
	t.Logf("%+v", decision)
}
