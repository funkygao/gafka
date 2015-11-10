package zk

import (
	"fmt"
)

type Controller struct {
	Broker   *Broker
	BrokerId int
	Epoch    string
}

func (c *Controller) String() string {
	return fmt.Sprintf("%8d %s epoch:%s", c.BrokerId, c.Broker.String(), c.Epoch)
}
