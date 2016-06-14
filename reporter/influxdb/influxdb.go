package influxdb

import (
	"fmt"
	"runtime/debug"
	"time"

	rp "github.com/funkygao/gafka/reporter"
	"github.com/influxdata/influxdb/client"
	"github.com/rcrowley/go-metrics"
)

var _ rp.Reporter = &reporter{}

type reporter struct {
	cf     *config
	reg    metrics.Registry
	client *client.Client

	quiting, quit chan struct{}
}

// New creates a InfluxDB reporter which will post the metrics from the given registry at each interval.
// CREATE RETENTION POLICY two_hours ON food_data DURATION 2h REPLICATION 1 DEFAULT
// SHOW RETENTION POLICIES ON food_data
// CREATE CONTINUOUS QUERY cq_30m ON food_data BEGIN SELECT mean(website) AS mean_website,mean(phone) AS mean_phone INTO food_data."default".downsampled_orders FROM orders GROUP BY time(30m) END
func New(r metrics.Registry, cf *config) *reporter {
	this := &reporter{
		reg:     r,
		cf:      cf,
		quiting: make(chan struct{}),
		quit:    make(chan struct{}),
	}

	return this
}

func (this *reporter) makeClient() (err error) {
	this.client, err = client.NewClient(client.Config{
		URL:      this.cf.url,
		Username: this.cf.username,
		Password: this.cf.password,
	})

	return
}

func (*reporter) Name() string {
	return "influxdb"
}

func (this *reporter) Stop() {
	close(this.quiting)
	<-this.quit
}

func (this *reporter) Start() error {
	if err := this.makeClient(); err != nil {
		return err
	}

	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()

	intervalTicker := time.Tick(this.cf.interval)
	for {
		select {
		case <-this.quiting:
			// flush
			this.writeInfluxDB(this.dump())
			close(this.quit)
			return nil

		case <-intervalTicker:
			this.writeInfluxDB(this.dump())

		}
	}

	return nil
}
