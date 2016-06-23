// Package hh provides a hinted handoff service for Pub.
package hh

type Service interface {
	Start() error
	Stop()

	Append(cluster, topic string, key, value []byte) error
}

var Default Service
