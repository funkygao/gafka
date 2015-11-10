package command

import (
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	clusterRoot = "/_kafka_clusters"
)

func zkConnect(zkAddr string) *zk.Conn {
	zkConn, _, err := zk.Connect(strings.Split(zkAddr, ","),
		time.Minute)
	if err != nil {
		panic(err)
	}

	return zkConn
}

func addCluster(zkConn *zk.Conn, name, path string) error {
	acl := zk.WorldACL(zk.PermAll)
	flags := int32(0)
	_, err := zkConn.Create(clusterRoot+"/"+name, []byte(path), flags, acl)
	return err
}

func getClusters(zkConn *zk.Conn) map[string]string {
	children, _, err := zkConn.Children(clusterRoot)
	if err != nil {
		panic(err)
	}

	r := make(map[string]string)
	for _, name := range children {
		path, _, err := zkConn.Get(clusterRoot + "/" + name)
		if err != nil {
			panic(err)
		}

		r[name] = string(path)
	}
	return r
}
