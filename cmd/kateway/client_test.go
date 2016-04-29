package main

import (
	"testing"
)

func TestClientsStateWithoutHaproxy(t *testing.T) {
	c := NewClientStates()
	r, _ := mockHttpRequest()
	ip := "1.1.1.1"
	port := "23445"
	r.RemoteAddr = ip + ":" + port
	r.Header.Del(HttpHeaderXForwardedFor)
	t.Log(getHttpRemoteIp(r))
	c.RegisterPubClient(r)
	t.Logf("%+v", c.Export())
	c.UnregisterPubClient(r.RemoteAddr)
	t.Logf("%+v", c.Export())
}

func TestClientsStateBehindHaproxy(t *testing.T) {
	c := NewClientStates()
	r, _ := mockHttpRequest()
	ip := "1.1.1.1"
	port := "23445"
	r.RemoteAddr = ip + ":" + port
	t.Log(getHttpRemoteIp(r))
	c.RegisterPubClient(r)
	t.Logf("%+v", c.Export())
	c.UnregisterPubClient(r.RemoteAddr)
	t.Logf("%+v", c.Export())
}
