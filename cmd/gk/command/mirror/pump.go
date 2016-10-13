package mirror

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/kafka-cg/consumergroup"
	log "github.com/funkygao/log4go"
)

func (this *Mirror) pump(sub *consumergroup.ConsumerGroup, pub sarama.AsyncProducer,
	stop, stopped chan struct{}) {
	defer func() {
		log.Info("closing sub...")
		sub.Close()

		stopped <- struct{}{} // notify others I'm done
	}()

	active := true
	backoff := time.Second * 2
	idle := time.Second * 10
	for {
		select {
		case <-this.quit:
			log.Trace("got signal quit")
			return

		case <-stop:
			// yes sir!
			log.Trace("got signal stop")
			return

		case <-time.After(idle):
			active = false
			log.Info("idle 10s waiting for new message")

		case msg, ok := <-sub.Messages():
			if !ok {
				log.Warn("sub encounters end of message stream")
				return
			}

			if !active || this.Debug {
				log.Info("<-[#%d] T:%s M:%s", this.transferN, msg.Topic, string(msg.Value))
			}
			active = true

			pub.Input() <- &sarama.ProducerMessage{
				Topic: msg.Topic,
				Key:   sarama.ByteEncoder(msg.Key),
				Value: sarama.ByteEncoder(msg.Value),
			}
			if this.AutoCommit {
				sub.CommitUpto(msg)
			}

			// rate limit, never overflood the limited bandwidth between IDCs
			// FIXME when compressed, the bandwidth calculation is wrong
			bytesN := len(msg.Topic) + len(msg.Key) + len(msg.Value) + 20 // payload overhead
			if this.bandwidthRateLimiter != nil && !this.bandwidthRateLimiter.Pour(bytesN) {
				log.Warn("%s -> bandwidth reached, backoff %s", gofmt.ByteSize(this.transferBytes), backoff)
				time.Sleep(backoff)
			}

			this.transferBytes += int64(bytesN)
			this.transferN++
			if this.transferN%this.ProgressStep == 0 {
				log.Info("%s %s %s", gofmt.Comma(this.transferN), gofmt.ByteSize(this.transferBytes), msg.Topic)
			}

		case err := <-sub.Errors():
			log.Error("quitting pump %v", err)
			return
		}
	}
}
