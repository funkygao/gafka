package command

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	"github.com/golang/snappy"
	"github.com/pmylund/sortutil"
)

var snappyMagic = []byte{130, 83, 78, 65, 80, 80, 89, 0} // SNAPPY

// TODO calculate how much data produced each day "github.com/hashicorp/go-memdb"
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
	cmdFlags.StringVar(&this.rootPath, "s", "", "")
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

// a kafka message:
// crc magic attributes keyLen key messageLen message
func (this *Segment) readSegment(filename string) {
	f, err := os.Open(filename) // readonly
	swallow(err)
	defer f.Close()

	const (
		maxKeySize = 10 << 10
		maxValSize = 2 << 20
	)

	var (
		buf = make([]byte, 12)
		key = make([]byte, maxKeySize)
		val = make([]byte, maxValSize)

		msgN        int64
		firstOffset uint64 = math.MaxUint64 // sentry
		endOffset   uint64
	)
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

		// offset, size, crc32, magic, attrs, key-len, key-content, msg-len, msg-content
		// crc32 = crc32(magic, attrs, key-len, key-content, msg-len, msg-content)

		// offset+size 8+4
		offset := binary.BigEndian.Uint64(buf[:8])
		size := binary.BigEndian.Uint32(buf[8:12])

		// crc32+magic+attrs+keySize[key] 4+1+1+4
		r.Read(buf[0:10])

		attr := buf[5]
		keySize := binary.BigEndian.Uint32(buf[6:10])
		if keySize > 0 && keySize != math.MaxUint32 {
			_, err = r.Read(key[:keySize])
			swallow(err)
		}

		// valSize[val] 4
		_, err = r.Read(buf[:4])
		swallow(err)
		valSize := binary.BigEndian.Uint32(buf[:4])
		if valSize > 0 {
			_, err = r.Read(val[:valSize])
			swallow(err)
		}

		switch sarama.CompressionCodec(attr) {
		case sarama.CompressionNone:
			fmt.Printf("offset:%d size:%d %s\n", offset, size, string(val[:valSize]))

		case sarama.CompressionGZIP:
			reader, err := gzip.NewReader(bytes.NewReader(val[:valSize]))
			swallow(err)
			v, err := ioutil.ReadAll(reader)
			swallow(err)

			fmt.Printf("offset:%d size:%d gzip %s\n", offset, size, string(v))

		case sarama.CompressionSnappy:
			v, err := this.snappyDecode(val[:valSize])
			swallow(err)

			fmt.Printf("offset:%d size:%d snappy %s\n", offset, size, string(v))
		}

		if firstOffset == math.MaxUint64 {
			firstOffset = offset
		}
		endOffset = offset
		msgN++
	}

	fmt.Printf("Total Messages: %d, %d - %d\n", msgN, firstOffset, endOffset)
}

func (*Segment) snappyDecode(src []byte) ([]byte, error) {
	if bytes.Equal(src[:8], snappyMagic) {
		var (
			pos   = uint32(16)
			max   = uint32(len(src))
			dst   = make([]byte, 0, len(src))
			chunk []byte
			err   error
		)
		for pos < max {
			size := binary.BigEndian.Uint32(src[pos : pos+4])
			pos += 4

			chunk, err = snappy.Decode(chunk, src[pos:pos+size])
			if err != nil {
				return nil, err
			}
			pos += size
			dst = append(dst, chunk...)
		}

		return dst, nil
	}

	return snappy.Decode(nil, src)
}

func (this *Segment) isKafkaLogSegment(fn string) bool {
	if !strings.HasSuffix(fn, ".log") || len(fn) != len("00000000000000000000.log") {
		return false
	}

	parts := strings.Split(fn, ".")
	if _, err := strconv.Atoi(parts[0]); err != nil {
		return false
	}

	return true
}

func (this *Segment) printSummary() {
	segments := make(map[string]map[int]map[int]int64) // dir:day:hour:size
	err := filepath.Walk(this.rootPath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() || !this.isKafkaLogSegment(f.Name()) {
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
	for dir := range segments {
		partitions = append(partitions, dir)
	}
	sort.Strings(partitions)

	type segment struct {
		partition string
		day       int
		hour      int
		size      int64
	}

	var maxSegment segment
	var totalSize int64
	for _, p := range partitions {
		summary := make([]segment, 0)
		for day, hourSize := range segments[p] {
			for hour, size := range hourSize {
				summary = append(summary, segment{
					partition: p,
					day:       day,
					hour:      hour,
					size:      size,
				})
			}
		}
		sortutil.AscByField(summary, "size")
		if this.limit > 0 && len(summary) > this.limit {
			summary = summary[:this.limit]
		}

		for _, s := range summary {
			if s.size > maxSegment.size {
				maxSegment = s
			}

			totalSize += s.size
			this.Ui.Output(fmt.Sprintf("%50s day:%2d hour:%2d size:%s", p,
				s.day, s.hour, gofmt.ByteSize(s.size)))
		}

	}

	this.Ui.Output(fmt.Sprintf("%50s day:%2d hour:%2d size:%s", "MAX-"+maxSegment.partition,
		maxSegment.day, maxSegment.hour, gofmt.ByteSize(maxSegment.size)))
	this.Ui.Output(fmt.Sprintf("%50s %s", "-TOTAL-", gofmt.ByteSize(totalSize)))

	return
}

func (*Segment) Synopsis() string {
	return "Scan the kafka segments and display summary"
}

func (this *Segment) Help() string {
	help := fmt.Sprintf(`
Usage: %s segment [options]

    %s

    -f segment file name

    -s dir
      Sumamry of a segment dir.
      Summary across partitions is supported if they have the same parent dir.

    -n limit
      Default unlimited.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
