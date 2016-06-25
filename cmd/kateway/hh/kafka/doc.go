// Package kafka implements a kafka-backend hinted handoff.
//
// When pub fails, kafka hinted handoff will publish to another
// cluster, and it continuously consumes the handoff cluster and
// pub to the original cluster.
package kafka
