package kafka

import (
	"time"

	log "github.com/funkygao/log4go"
)

func (this *jobPool) runWorker() {
	log.Trace("job worker started")

	for {
		select {
		case queue := <-this.newQueues:
			log.Info("job pool found new queue: %s", queue)

			go this.pumpQueue(queue)
		}
	}

}

func (this *jobPool) pumpQueue(queue string) {
	c, err := this.pool.Get()
	if err != nil {
		log.Error(err)
		return // TODO retry
	}

	for {
		job, err := c.Get(time.Second, queue)
		if err != nil {
			log.Error(err)
			continue
		}

		log.Debug("%s %s", job.Id(), string(job.Data))
	}
}
