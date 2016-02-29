package zk

import (
	"fmt"

	"github.com/funkygao/gafka/zk"
)

func Root(zone string) string {
	return fmt.Sprintf("%s/%s", zk.KatewayIdsRoot, zone)
}
