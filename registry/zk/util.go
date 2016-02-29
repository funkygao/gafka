package zk

import (
	"fmt"
)

func Root(zone string) string {
	return fmt.Sprintf("%s/%s", zk.KatewayIdsRoot, zone)
}
