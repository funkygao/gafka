package protos

import (
	"bufio"
	"io"
	"net/http"
	"time"

	"github.com/funkygao/gocli"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
)

func Assembler(prot string, serverPort int, ui cli.Ui) *tcpassembly.Assembler {
	streamFactory := &tcpStreamFactory{
		protocol:   prot,
		serverPort: serverPort,
		ui:         ui,
	}
	streamPool := tcpassembly.NewStreamPool(streamFactory)
	return tcpassembly.NewAssembler(streamPool)
}

type tcpStreamFactory struct {
	protocol   string
	serverPort int
	ui         cli.Ui
}

func (factory *tcpStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	s := &tcpStream{
		net:        net,
		transport:  transport,
		startedAt:  time.Now(),
		r:          tcpreader.NewReaderStream(),
		protocol:   factory.protocol,
		serverPort: factory.serverPort,
		ui:         factory.ui,
	}
	go s.run()

	return &s.r
}

type tcpStream struct {
	net, transport gopacket.Flow
	startedAt      time.Time
	r              tcpreader.ReaderStream

	protocol   string
	serverPort int
	ui         cli.Ui
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
