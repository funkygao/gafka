// exaplains zookeeper session expires mechanism.
// The code is extracted from zookeeper server code.
//
// Session expiration is managed by the ZooKeeper cluster itself, not by the client.
//
// When the ZK client establishes a session with the cluster it provides a "timeout" value.
// This value is used by the cluster to determine when the client's session expires.
//
// Expirations happens when the cluster does not hear from the client within the
// specified session timeout period (i.e. no heartbeat).
//
// At session expiration the cluster will delete any/all ephemeral nodes owned by that session and
// immediately notify any/all connected clients of the change (anyone watching those znodes).
// At this point the client of the expired session is still disconnected from the cluster, it
// will not be notified of the session expiration until/unless it is able to re-establish a
// connection to the cluster.
// The client will stay in disconnected state until the TCP connection is re-established with the
// cluster, at which point the watcher of the expired session will receive the "session expired"
// notification.

package main

import (
	"flag"
	"fmt"
)

var (
	tickTime          = 3000 // zoo.cfg, default 3s
	minSessionTimeout = -1   // zoo.cfg
	maxSessionTimeout = -1   // zoo.cfg
)

func getMinSessionTimeout() int {
	if minSessionTimeout == -1 {
		return tickTime * 2
	} else {
		return minSessionTimeout
	}
}

func getMaxSessionTimeout() int {
	if maxSessionTimeout == -1 {
		return tickTime * 20
	} else {
		return maxSessionTimeout
	}
}

func getRealSessionTimeout(clientSessionTimeout int) int {
	var sessionTimeout = clientSessionTimeout
	min, max := getMinSessionTimeout(), getMaxSessionTimeout()
	if sessionTimeout < min {
		sessionTimeout = min
	}
	if sessionTimeout > max {
		sessionTimeout = max
	}

	return sessionTimeout
}

func main() {
	var clientConfiggedSessionTimeout int
	flag.IntVar(&clientConfiggedSessionTimeout, "c", 30*1000, "client side zk session timeout, which will be sent to zk for negotiation")
	flag.IntVar(&tickTime, "t", 3000, "tickTime in zoo.cfg")
	flag.Parse()

	fmt.Printf("tickTime=%ds\n", tickTime/1000)
	fmt.Printf("default between: %ds ~ %ds\n",
		getRealSessionTimeout(-1)/1000,
		getRealSessionTimeout(1<<30)/1000)

	fmt.Printf("client sent:%ds => got %ds\n",
		clientConfiggedSessionTimeout/1000,
		getRealSessionTimeout(clientConfiggedSessionTimeout)/1000)
}
