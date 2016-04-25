package main

type Cluster struct {
	brokers map[int]Broker // brokerId:Broker
}
