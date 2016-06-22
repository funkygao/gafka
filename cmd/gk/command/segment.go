package command

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	"github.com/pmylund/sortutil"
)

type Segment struct {
	Ui  cli.Ui
	Cmd string

	rootPath string
	filename string
	limit    int
}

func (this *Segment) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("segment", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.rootPath, "p", "", "")
	cmdFlags.IntVar(&this.limit, "n", -1, "")
	cmdFlags.StringVar(&this.filename, "f", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.rootPath != "" {
		this.printSummary()
		return
	}

	if validateArgs(this, this.Ui).
		require("-f").
		invalid(args) {
		return 2
	}

	this.readSegment(this.filename)

	return
}

func (this *Segment) readSegment(filename string) {
	f, err := os.Open(filename) // readonly
	swallow(err)
	defer f.Close()

	var buf = make([]byte, 12)
	r := bufio.NewReader(f)
	for {
		_, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}

		offset := binary.BigEndian.Uint64(buf[:8])
		size := binary.BigEndian.Uint32(buf[8:12])

		// crc32+magic+attr+key
		r.Read(buf[0:10])

		attr := buf[5]
		keySize := binary.BigEndian.Uint32(buf[6:10])
		if keySize > 0 && keySize != math.MaxUint32 {
			for i := 0; i < int(keySize); i++ {
				r.ReadByte()
			}
		}

		r.Read(buf[:4])
		valSize := binary.BigEndian.Uint32(buf[:4])

		if valSize > 0 {
			println(valSize)
			val := make([]byte, int(valSize))
			r.Read(val)
			this.Ui.Output(string(val))
		}

		this.Ui.Output(fmt.Sprintf("off:%d size:%d %v %d %d", offset, size, attr,
			keySize, valSize))

		switch sarama.CompressionCodec(attr) {
		case sarama.CompressionGZIP:
		case sarama.CompressionNone:
		case sarama.CompressionSnappy:
			println("snappy")
		}

	}
}

func (this *Segment) printSummary() {
	segments := make(map[string]map[int]map[int]int64) // dir:day:hour:size
	err := filepath.Walk(this.rootPath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		if !strings.HasSuffix(f.Name(), ".index") && !strings.HasSuffix(f.Name(), ".log") {
			return nil
		}
		if !strings.HasSuffix(f.Name(), ".log") {
			return nil
		}

		dir := filepath.Base(filepath.Dir(path))
		if _, present := segments[dir]; !present {
			segments[dir] = make(map[int]map[int]int64)
		}
		if _, present := segments[dir][f.ModTime().Day()]; !present {
			segments[dir][f.ModTime().Day()] = make(map[int]int64)
		}
		segments[dir][f.ModTime().Day()][f.ModTime().Hour()] += f.Size()
		return nil
	})
	if err != nil {
		this.Ui.Error(err.Error())
	}

	partitions := make([]string, 0, len(segments))
	for dir, _ := range segments {
		partitions = append(partitions, dir)
	}
	sort.Strings(partitions)

	type segment struct {
		day  int
		hour int
		size int64
	}

	for _, p := range partitions {
		summary := make([]segment, 0)
		for day, hourSize := range segments[p] {
			for hour, size := range hourSize {
				summary = append(summary, segment{
					day:  day,
					hour: hour,
					size: size,
				})
			}
		}
		sortutil.AscByField(summary, "size")
		if this.limit > 0 && len(summary) > this.limit {
			summary = summary[:this.limit]
		}
		for _, s := range summary {
			this.Ui.Output(fmt.Sprintf("%30s day:%2d hour:%2d size:%s", p,
				s.day, s.hour, gofmt.ByteSize(s.size)))
		}

	}

	return
}

func (*Segment) Synopsis() string {
	return "Scan the kafka segments and display summary"
}

func (this *Segment) Help() string {
	help := fmt.Sprintf(`
Usage: %s segment [options]

    Scan the kafka segments and display summary

    -f segment file name

    -p dir
      Sumamry of a segment dir.
      Summary across partitions is supported if they have the same parent dir.

    -n limit
      Default unlimited.

`, this.Cmd)
	return strings.TrimSpace(help)
}
