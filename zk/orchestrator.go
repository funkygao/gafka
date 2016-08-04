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

func (this *Orchestrator) WatchJobs() (JobList, <-chan zk.Event, error) {
	this.connectIfNeccessary()

	children, _, c, err := this.conn.ChildrenW(PubsubJobs)
	if err != nil {
		return nil, nil, err
	}

	r := make(JobList, 0, len(children))
	for _, job := range children {
		r = append(r, job)
	}
	return r, c, nil
}

func (this *Orchestrator) ClaimJob() {

}

func (this *Orchestrator) ReleaseJob() {

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

type JobList []string

func (this JobList) Len() int {
	return len(this)
}

func (this JobList) Less(i, j int) bool {
	return this[i] < this[j]
}

func (this JobList) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}
