package consumergroup

import (
	"errors"
)

var (
	AlreadyClosing = errors.New("The consumer group is already shutting down.")
	UncleanClose   = errors.New("Not all offsets were committed before shutdown was completed")
)
