package main

import (
	"net"
	"net/http"
	"sync"

	log "github.com/funkygao/log4go"
)

type ClientStates struct {
	// client states TODO differetiate ws and normal client
	pubMap         map[string]string   // haproxyHostPort=>realHostPort
	pubClients     map[string]struct{} // key is realHostPort
	pubClientsLock sync.RWMutex

	subMap         map[string]string   // haproxyHostPort=>realHostPort
	subClients     map[string]struct{} // key is realHostPort
	subClientsLock sync.RWMutex
}

func NewClientStates() *ClientStates {
	this := &ClientStates{
		pubMap:     make(map[string]string, 1000),
		pubClients: make(map[string]struct{}, 1000),
		subMap:     make(map[string]string, 1000),
		subClients: make(map[string]struct{}, 1000),
	}

	return this
}

func (this *ClientStates) Reset() {
	this.pubClientsLock.Lock()
	this.pubMap = make(map[string]string, 1000)
	this.pubClients = make(map[string]struct{}, 1000)
	this.pubClientsLock.Unlock()

	this.subClientsLock.Lock()
	this.subMap = make(map[string]string, 1000)
	this.subClients = make(map[string]struct{}, 1000)
	this.subClientsLock.Unlock()
}

func (this *ClientStates) RegisterPubClient(r *http.Request) {
	realIp := getHttpRemoteIp(r)
	if realIp == r.RemoteAddr {
		return
	}

	_, port, _ := net.SplitHostPort(r.RemoteAddr) // FIXME port is not real port
	realHostPort := realIp + ":" + port

	this.pubClientsLock.Lock()
	this.pubMap[r.RemoteAddr] = realHostPort
	this.pubClients[realHostPort] = struct{}{}
	this.pubClientsLock.Unlock()
}

func (this *ClientStates) UnregisterPubClient(c net.Conn) {
	haproxyHostPort := c.RemoteAddr().String()

	this.pubClientsLock.Lock()
	realHostPort := this.pubMap[haproxyHostPort]
	if realHostPort == "" {
		log.Warn("pub try to unregister a non-exist haproxy client: %s", haproxyHostPort)

		this.pubClientsLock.Unlock()
		return
	}

	delete(this.pubMap, haproxyHostPort)
	delete(this.pubClients, realHostPort)
	this.pubClientsLock.Unlock()
}

func (this *ClientStates) RegisterSubClient(r *http.Request) {
	realIp := getHttpRemoteIp(r)
	if realIp == r.RemoteAddr {
		return
	}

	_, port, _ := net.SplitHostPort(r.RemoteAddr)
	realHostPort := realIp + ":" + port

	this.subClientsLock.Lock()
	this.subMap[r.RemoteAddr] = realHostPort
	this.subClients[realHostPort] = struct{}{}
	this.subClientsLock.Unlock()
}

func (this *ClientStates) UnregisterSubClient(c net.Conn) {
	haproxyHostPort := c.RemoteAddr().String()

	this.subClientsLock.Lock()
	realHostPort := this.subMap[haproxyHostPort]
	if realHostPort == "" {
		log.Warn("sub try to unregister a non-exist haproxy client: %s", haproxyHostPort)

		this.subClientsLock.Unlock()
		return
	}

	delete(this.subMap, haproxyHostPort)
	delete(this.subClients, realHostPort)
	this.subClientsLock.Unlock()
}

func (this *ClientStates) Export() map[string][]string {
	r := make(map[string][]string)

	this.pubClientsLock.RLock()
	pubClients := this.pubClients
	this.pubClientsLock.RUnlock()
	r["pub"] = make([]string, 0, len(pubClients))
	for realHostPort, _ := range pubClients {
		r["pub"] = append(r["pub"], realHostPort)
	}

	this.subClientsLock.RLock()
	subClients := this.subClients
	this.subClientsLock.RUnlock()
	r["sub"] = make([]string, 0, len(subClients))
	for realHostPort, _ := range subClients {
		r["sub"] = append(r["sub"], realHostPort)
	}
	return r
}
