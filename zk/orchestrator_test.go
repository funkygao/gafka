package zk

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/ctx"
	"github.com/samuel/go-zookeeper/zk"
)

func init() {
	ctx.LoadFromHome()
}

func TestOrchestatorAll(t *testing.T) {
	zkzone := NewZkZone(DefaultConfig(ctx.DefaultZone(), ctx.ZoneZkAddrs(ctx.DefaultZone())))
	defer zkzone.Close()

	actorId := "actor1"
	o := zkzone.NewOrchestrator()
	err := o.RegisterActor(actorId)
	assert.Equal(t, nil, err)
	ok, err := o.ActorRegistered(actorId)
	assert.Equal(t, true, ok)
	assert.Equal(t, nil, err)
	ok, err = o.ActorRegistered(actorId + "non-exist")
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, err)

	w, c, err := o.WatchActors()
	assert.Equal(t, true, c != nil)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(w))
	assert.Equal(t, actorId, w[0])

	err = zkzone.CreateJob("cluster", "topic")
	if err != nil {
		assert.Equal(t, zk.ErrNodeExists, err)
	} else {
		j, c, err := o.WatchJobs()
		assert.Equal(t, nil, err)

		assert.Equal(t, true, c != nil)
		assert.Equal(t, true, len(j) >= 1)
	}
}
