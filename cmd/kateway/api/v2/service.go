package pubsub

import (
	"golang.org/x/net/context"
)

//
type service interface {
	publishMessage(ctx context.Context, topic string, key, value []byte)
	fetchMessages(ctx context.Context)
	addJob(ctx context.Context)
	acknowledge(ctx context.Context)
}
