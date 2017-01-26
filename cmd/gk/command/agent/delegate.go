package agent

import (
	"encoding/json"
	"sync"

	log "github.com/funkygao/log4go"
	"github.com/hashicorp/memberlist"
)

var _ memberlist.Delegate = &delegate{}

type delegate struct {
	mu    sync.RWMutex
	items map[string]string

	ctx *Agent
}

func newDelegate(ctx *Agent) *delegate {
	return &delegate{
		items: make(map[string]string),
		ctx:   ctx,
	}
}

// used to retrive meta-data about the current node when broadcasting an alive message.
func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

// when a user-data message is received.
func (d *delegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		log.Warn("got empty notify message, ignored")
		return
	}

	log.Trace("%+v", b)

	switch b[0] {
	case 'd': // data
		// update local state according to the user-data message
	}

}

// for user-data messages to be broadcast.
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.ctx.broadcasts.GetBroadcasts(overhead, limit)
}

// for a TCP Push/Pull.
func (d *delegate) LocalState(join bool) []byte {
	d.mu.RLock()
	m := d.items
	d.mu.RUnlock()
	b, _ := json.Marshal(m)
	return b
}

// invoked after a TCP Push/Pull.
func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	log.Trace("%+v %+v", buf, join)

	if len(buf) == 0 {
		return
	}
	if !join {
		return
	}

	var m map[string]string
	if err := json.Unmarshal(buf, &m); err != nil {
		return
	}

	// merge
	d.mu.Lock()
	for k, v := range m {
		d.items[k] = v
	}
	d.mu.Unlock()
}
