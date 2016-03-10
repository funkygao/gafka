package xa

import (
	"time"
)

type State byte

const (
	_ = iota
	StatePrepare
	StateCommit
	StateRollback
)

type TxStateTable struct {
	Offset int64
	Time   time.Time
	State  State
}
