package protos

import (
	"bufio"
	"io"
	"net/http"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
)

func Assembler() *tcpassembly.Assembler {
	streamFactory := &tcpStreamFactory{}
	streamPool := tcpassembly.NewStreamPool(streamFactory)
	return tcpassembly.NewAssembler(streamPool)
}

type tcpStreamFactory struct{}

func (factory *tcpStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	s := &tcpStream{
		net:       net,
		transport: transport,
		startedAt: time.Now(),
		r:         tcpreader.NewReaderStream(),
	}
	go s.run()

	return &s.r
}

type tcpStream struct {
	net, transport gopacket.Flow
	startedAt      time.Time
	r              tcpreader.ReaderStream
}

func (s *tcpStream) run() {
	buf := bufio.NewReader(&s.r)
	for {
		// kafka read request from buf
		req, err := http.ReadRequest(buf)
		if err == io.EOF {
			return
		} else if err != nil {
			println(err)
		} else {
			println(req)
		}

	}
}
