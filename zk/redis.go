package zk

import (
	"encoding/json"
	"fmt"
	"path"
)

type RedisInstance struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func (ri RedisInstance) String() string {
	return fmt.Sprintf("%s:%d", ri.Host, ri.Port)
}

type RedisCluster struct {
	Name       string          `json:"name"`
	Desciption string          `json:"desc"`
	Members    []RedisInstance `json:"members"`
}

func (this *ZkZone) redisZpath(host string, port int) string {
	return fmt.Sprintf("%s/%s:%d", RedisMonPath, host, port)
}

func (this *ZkZone) redisClusterPath(cluster string) string {
	return path.Join(RedisClusterRoot, cluster)
}

func (this *ZkZone) AddRedis(host string, port int) {
	this.connectIfNeccessary()

	this.createZnode(this.redisZpath(host, port), nil)
}

func (this *ZkZone) DelRedis(host string, port int) {
	this.connectIfNeccessary()

	this.conn.Delete(this.redisZpath(host, port), -1)
}

func (this *ZkZone) AllRedis() []string {
	this.connectIfNeccessary()

	return this.children(RedisMonPath)
}

func (this *ZkZone) AddRedisCluster(cluster string, instances []RedisInstance) {
	this.connectIfNeccessary()

	data, _ := json.Marshal(RedisCluster{
		Name:    cluster,
		Members: instances,
	})

	this.createZnode(this.redisClusterPath(cluster), data)
}

func (this *ZkZone) DelRedisCluster(cluster string) {
	this.connectIfNeccessary()

	this.conn.Delete(this.redisClusterPath(cluster), -1)
}

func (this *ZkZone) AllRedisClusters() []RedisCluster {
	this.connectIfNeccessary()

	r := make([]RedisCluster, 0)
	for _, cluster := range this.children(RedisClusterRoot) {
		data, _, err := this.conn.Get(this.redisClusterPath(cluster))
		if err != nil {
			panic(err)
		}

		var c RedisCluster
		json.Unmarshal(data, &c)
		r = append(r, c)
	}
	return r
}
