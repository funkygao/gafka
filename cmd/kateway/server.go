package main

import (
	"time"
)

var (
	httpReadTimeout    = time.Minute * 5 // TODO
	httpWriteTimeout   = time.Minute
	httpHeaderMaxBytes = 4 << 10
)
