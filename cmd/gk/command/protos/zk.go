package protos

import (
	"fmt"
)

// all the zookeeper network protocol serialize/unserialize follows zookeeper.jute.
// the zk struct only applies on client<->zk server, zk<->zk not included.
//
// len request header request body
// len response header response body
//
// connect request
// 00 00 00 44 00 00 00 00 00 00 00 00 00 00 00 00 00 00 78 32 00 00 00 00 00 00 00 00 00 00 00 16 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
// =========== =========== ======================= =========== ======================= =========== ===============================================
// len         ver         zxid                    timeout     SessionID               passwd len  passwd
//
// connect response
// 00 00 00 36 00 00 00 00 00 00 78 32 01 89 100 172 109 107 00 08 00 00 00 16 48 75 120 99 216 146 206 252 119 224 21 170 126 60 63 235
// =========== =========== =========== =========================== =========== =========================================================
// len         ver         timeout     SessionID                   passwd len  passwd
//
// get children /
// 00 00 00 14 00 00 00 01 00 00 00 12 00 00 00 01 47 00
// =========== =========== =========== =========== == ==
// len         xid         opcode      path len    path watch
//
type zk struct {
	serverPort int
}

func (z *zk) Unmarshal(payload []byte) string {
	r := ""
	for _, b := range payload {
		r += fmt.Sprintf("%02d ", b)
	}
	return r
}
