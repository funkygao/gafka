package zk

import (
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"time"
)

var PanicHandler func(interface{})

func TimestampToTime(ts string) time.Time {
	sec, _ := strconv.ParseInt(ts, 10, 64)
	if sec > 143761237100 {
		sec /= 1000
	}

	return time.Unix(sec, 0)
}

func withRecover(fn func()) {
	defer func() {
		handler := PanicHandler
		if handler != nil {
			if err := recover(); err != nil {
				handler(err)
			}
		}
	}()

	fn()
}

func hostOfConsumer(consumerId string) string {
	// consumer id
	//   java: $group_$hostname-$timestamp-$uuidSignificantBits
	// golang: $kateway_ip@real_ip:$uuidFull

	if strings.Contains(consumerId, ":") {
		// golang client
		p := strings.SplitN(consumerId, ":", 2)
		return p[0]
	}

	dashN := 0
	var lo, hi int
	for hi = len(consumerId) - 1; hi >= 0; hi-- {
		if consumerId[hi] == '-' {
			dashN++
			if dashN == 2 {
				break
			}
		}
	}

	for lo = hi; lo >= 0 && consumerId[lo] != '_'; lo-- {
	}

	return consumerId[lo+1 : hi]
}

// zkFourLetterWord execute ZooKeeper Commands: The Four Letter Words
// conf, cons, crst, envi, ruok, stat, wchs, wchp
func zkFourLetterWord(server, command string, timeout time.Duration) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", server, timeout)

	if err != nil {
		return nil, err
	}

	// the zookeeper server should automatically close this socket
	// once the command has been processed, but better safe than sorry
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(timeout))

	_, err = conn.Write([]byte(command))
	if err != nil {
		return nil, err
	}

	conn.SetReadDeadline(time.Now().Add(timeout))

	resp, err := ioutil.ReadAll(conn)

	if err != nil {
		return nil, err
	}

	return resp, nil
}
