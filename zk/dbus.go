package zk

import (
	"path"
)

func DbusCheckpointRoot(cluster string) string {
	return path.Join(DbusRoot, cluster, dbusCheckpointPath)
}

func DbusConfig(cluster string) string {
	return path.Join(DbusRoot, cluster, dbusConfPath)
}

func DbusConfigDir(cluster string) string {
	return path.Join(DbusRoot, cluster, dbusConfDirPath)
}

func DbusClusterRoot(cluster string) string {
	return path.Join(DbusRoot, cluster, dbusClusterPath)
}
