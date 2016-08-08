package controller

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/zk"
)

func TestAssignResourcesToActors_Normal(t *testing.T) {
	jobs := zk.ResourceList([]string{"a", "b", "c", "d", "e"})
	actors := zk.ActorList([]string{"1", "2"})

	decision := assignResourcesToActors(actors, jobs)
	t.Logf("%+v", decision)
	assert.Equal(t, 3, len(decision["1"]))
	assert.Equal(t, 2, len(decision["2"]))
}

func TestAssignResourcesToActors_EmptyJobQueues(t *testing.T) {
	jobs := zk.ResourceList([]string{})
	actors := zk.ActorList([]string{"1", "2"})

	decision := assignResourcesToActors(actors, jobs)
	t.Logf("%+v", decision)
}

func TestAssignResourcesToActors_ActorMoreThanJobQueues(t *testing.T) {
	jobs := zk.ResourceList([]string{"job1"})
	actors := zk.ActorList([]string{"1", "2"})

	decision := assignResourcesToActors(actors, jobs)
	t.Logf("%+v", decision)
	assert.Equal(t, 0, len(decision["2"]))
	assert.Equal(t, 1, len(decision["1"]))
}
