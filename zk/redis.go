package zk

import (
	"fmt"
)

func (this *ZkZone) redisZpath(host string, port int) string {
	return fmt.Sprintf("%s/%s:%d", RedisMonPath, host, port)
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
