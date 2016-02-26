package main

import (
	"net"
	"net/http"
	"sync"
)

type ClientStates struct {
	// client states TODO differetiate ws and normal client
	pubClients     map[string]struct{}
	pubClientsLock sync.Mutex
	subClients     map[string]struct{}
	subClientsLock sync.Mutex
}

func NewClientStates() *ClientStates {
	this := &ClientStates{
		pubClients: make(map[string]struct{}, 1000),
		subClients: make(map[string]struct{}, 1000),
	}

	return this
}

func (this *ClientStates) RegisterPubClient(r *http.Request) {
	realIp := getHttpRemoteIp(r)
	if realIp == r.RemoteAddr {
		return
	}

	haproxyIp, port := net.SplitHostPort(r.RemoteAddr)
	this.pubClientsLock.Lock()
	this.pubClients[realIp+":"+port] = struct{}{}
	this.pubClientsLock.Unlock()
}

func (this *ClientStates) RegisterSubClient(r *http.Request) {

}

func (this *ClientStates) UnregisterPubClient(c net.Conn) {
	haproxyIp, port := net.SplitHostPort(c.RemoteAddr().String())
}

func (this *ClientStates) UnregisterSubClient(c net.Conn) {

}

func (this *ClientStates) Export() map[string][]string {

}
