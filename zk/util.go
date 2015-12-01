package zk

import (
	"io/ioutil"
	"net"
	"strconv"
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
