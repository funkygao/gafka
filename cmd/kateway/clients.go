package main

import (
	"net"
	"net/http"
	"sync"
)

type ClientStates struct {
	// client states TODO differetiate ws and normal client
	pubClients     map[string]struct{}
	pubClientsLock sync.RWMutex

	subClients     map[string]struct{}
	subClientsLock sync.RWMutex
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
	r := make(map[string][]string)
	r["pub"] = make([]string, 0)
	r["sub"] = make([]string, 0)
	this.pubClientsLock.RLock()
	pubClients := this.pubClients
	this.pubClientsLock.RUnlock()
	for ipPort, _ := range pubClients {
		r["pub"] = append(r["pub"], ipPort)
	}

	this.subClientsLock.RLock()
	subClients := this.subClientsLock
	this.subClientsLock.RUnlock()
	for ipPort, _ := range subClients {
		r["sub"] = append(r["sub"], ipPort)
	}
	return r
}
