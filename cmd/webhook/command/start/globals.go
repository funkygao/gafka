package start

import (
	"fmt"

	"github.com/funkygao/gafka"
)

var (
	UserAgent = fmt.Sprintf("%s/%s-%s", "webhooker", gafka.Version, gafka.BuildId)
)
