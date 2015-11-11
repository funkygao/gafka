package zk

import (
	"fmt"
)

type Controller struct {
	Broker *Broker
	Epoch  string
}

func (c *Controller) String() string {
	return fmt.Sprintf("%8s epoch:%s %s", c.Broker.Id, c.Epoch, c.Broker.String())
}
