package main

import (
	"log"
)

type ZookeeperLeaderElector struct {
	brokerId int
	leaderId int

	zkClient *ZkClient

	electionPath        string
	onBecomingLeader    func()
	onResigningAsLeader func()
}

func (this *ZookeeperLeaderElector) Startup() {
	this.leaderId = -1

	this.zkClient.subscribeDataChanges(this.electionPath, this.leaderChangeListener)

	// elect mysefl as the controller
	this.elect()
}

func (this *ZookeeperLeaderElector) elect() {
	if err := createEphemeralPath(this.electionPath); err == nil {
		log.Printf("%d successfully elected as leader", this.brokerId)
		this.leaderId = this.brokerId
		this.onBecomingLeader()
	} else {
		if err == ZkNodeExistsException {
			this.leaderId = this.zkClient.readData(this.electionPath)
			if this.leaderId == -1 {
				log.Printf("A leader has been elected but just resigned, this will result in another round of election")
				return
			} else {
				log.Printf("Broker %d was elected as leader", this.leaderId)
			}
		} else {
			// unkown err
			this.resign()
		}

	}

}

func (this *ZookeeperLeaderElector) resign() {
	this.leaderId = -1
	this.zkClient.deletePath(this.electionPath)
}

func (this *ZookeeperLeaderElector) leaderChangeListener() {
	if znodeDataChanged {
		this.leaderId = this.zkClient.readData(this.electionPath)
	} else if znodeDeleted {
		if this.leaderId == this.brokerId {
			this.onResigningAsLeader()
		}

		this.elect()
	}
}
