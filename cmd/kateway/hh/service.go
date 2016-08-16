// Package hh provides a hinted handoff service for Pub.
package hh

type Service interface {
	Start() error
	Stop()

	Append(cluster, topic string, key string, value []byte) error
	Empty(cluster, topic string) bool
}

var Default Service
