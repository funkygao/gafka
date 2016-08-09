package zk

import (
	"fmt"

	"github.com/samuel/go-zookeeper/zk"
)

type Orchestrator struct {
	*ZkZone
}

func (this *ZkZone) NewOrchestrator() *Orchestrator {
	return &Orchestrator{ZkZone: this}
}

func (this *Orchestrator) ActorRegistered(id string) (bool, error) {
	this.connectIfNeccessary()

	path := fmt.Sprintf("%s/%s", PubsubActors, id)
	r, _, err := this.conn.Exists(path)
	return r, err
}

func (this *Orchestrator) RegisterActor(id string, val []byte) error {
	path := fmt.Sprintf("%s/%s", PubsubActors, id)
	return this.CreateEphemeralZnode(path, val)
}

func (this *Orchestrator) ResignActor(id string) error {
	path := fmt.Sprintf("%s/%s", PubsubActors, id)
	return this.conn.Delete(path, -1)
}

func (this *Orchestrator) WatchActors() (ActorList, <-chan zk.Event, error) {
	this.connectIfNeccessary()

	children, _, c, err := this.conn.ChildrenW(PubsubActors)
	if err != nil {
		return nil, nil, err
	}

	r := make(ActorList, 0, len(children))
	for _, actor := range children {
		r = append(r, actor)
	}
	return r, c, nil
}

func (this *Orchestrator) WatchResources(path string) (ResourceList, <-chan zk.Event, error) {
	this.connectIfNeccessary()

	children, _, c, err := this.conn.ChildrenW(path)
	if err != nil {
		return nil, nil, err
	}

	r := make(ResourceList, 0, len(children))
	for _, job := range children {
		r = append(r, job)
	}
	return r, c, nil
}

func (this *Orchestrator) ClaimResource(actorId, root, resource string) (err error) {
	this.connectIfNeccessary()

	path := fmt.Sprintf("%s/%s", root, resource)
	this.ensureParentDirExists(path)
	err = this.CreateEphemeralZnode(path, []byte(actorId))
	if err == zk.ErrNodeExists {
		data, _, err := this.conn.Get(path)
		if err != nil {
			return err
		}
		if string(data) != actorId {
			return ErrClaimedByOthers
		}
		return nil
	}

	return
}

func (this *Orchestrator) ReleaseResource(actorId, root, resource string) error {
	path := fmt.Sprintf("%s/%s", root, resource)
	data, _, err := this.conn.Get(path)
	if err != nil && err != zk.ErrNoNode {
		return err
	}

	if data == nil || string(data) != actorId {
		return ErrNotClaimed
	}

	return this.conn.Delete(path, -1)
}

type ActorList []string

func (this ActorList) Len() int {
	return len(this)
}

func (this ActorList) Less(i, j int) bool {
	return this[i] < this[j]
}

func (this ActorList) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

type ResourceList []string

func (this ResourceList) Len() int {
	return len(this)
}

func (this ResourceList) Less(i, j int) bool {
	return this[i] < this[j]
}

func (this ResourceList) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}
