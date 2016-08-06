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

func (this *Orchestrator) RegisterActor(id string) error {
	path := fmt.Sprintf("%s/%s", PubsubActors, id)
	return this.CreateEphemeralZnode(path, nil)
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

func (this *Orchestrator) WatchJobQueues() (JobQueueList, <-chan zk.Event, error) {
	this.connectIfNeccessary()

	children, _, c, err := this.conn.ChildrenW(PubsubJobQueues)
	if err != nil {
		return nil, nil, err
	}

	r := make(JobQueueList, 0, len(children))
	for _, job := range children {
		r = append(r, job)
	}
	return r, c, nil
}

func (this *Orchestrator) ClaimJobQueue(jobQueue string) (err error) {
	return
}

func (this *Orchestrator) ReleaseJobQueue(jobQueue string) {

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

type JobQueueList []string

func (this JobQueueList) Len() int {
	return len(this)
}

func (this JobQueueList) Less(i, j int) bool {
	return this[i] < this[j]
}

func (this JobQueueList) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}
