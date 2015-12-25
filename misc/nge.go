// logstashed nginx error log parser
package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/funkygao/golib/color"
	sqldb "github.com/funkygao/golib/db"
)

type NgType int

const (
	TypeIntra NgType = iota
	TypeExtra
)

var (
	logger          *log.Logger
	dbIntra         *sqldb.SqlDb
	dbExtra         *sqldb.SqlDb
	intraInsertStmt *sql.Stmt
	extraInsertStmt *sql.Stmt

	debugMode bool
	dryrun    bool

	progressStep int

	normalizerNum = regexp.MustCompile(`\d+`)
)

type logLine struct {
	Timestamp string `json:"@timestamp"`
	Version   string `json:"@version"`
	Type      string `json:"type"`
	Path      string `json:"path"`

	Host    string `json:"host"`
	Message string `json:"message"`
}

func (this *logLine) save(t NgType) {
	var stmt *sql.Stmt
	switch t {
	case TypeExtra:
		stmt = extraInsertStmt
	case TypeIntra:
		stmt = intraInsertStmt
	}

	// message: 2015/12/25 06:47:15 [error] 140915#0: check protocol http error with peer: 10.209.37.33:10085
	p := strings.SplitN(this.Message, " ", 5)
	msg := normalizerNum.ReplaceAll([]byte(string(p[4])), []byte("?"))
	if dryrun {
		fmt.Println(string(msg))
		return
	}

	if _, err := stmt.Exec(this.Host, time.Now().Unix(), string(msg)); err != nil {
		fmt.Println(err)
	}
}

func init() {
	logger = log.New(os.Stdout, "", log.LstdFlags)
	prepareDB()
}

func main() {
	flag.BoolVar(&debugMode, "d", false, "debug mode")
	flag.BoolVar(&dryrun, "dryrun", false, "dryrun mode")
	flag.IntVar(&progressStep, "step", 100, "show progress step")
	flag.Parse()

	var (
		errinfo logLine
		jsonIdx int
		ngType  NgType
		n       int64
	)
	for line := range EachLogLine() {
		n++
		if progressStep > 0 && n%int64(progressStep) == 0 {
			fmt.Printf("%d errors found\n", n)
		}

		if strings.Contains(string(line[:30]), "extra") {
			ngType = TypeExtra
		} else {
			ngType = TypeIntra
		}

		jsonIdx = bytes.IndexByte(line, '{')
		if err := json.Unmarshal(line[jsonIdx:], &errinfo); err != nil {
			fmt.Printf("%s: %s", color.Red(err.Error()), string(line[jsonIdx:]))
			continue
		}
		if debugMode {
			fmt.Printf("%#v\n", errinfo)
		}

		errinfo.save(ngType)
	}

	fmt.Println("bye")
}

func prepareDB() {
	intraDsn := fmt.Sprintf("file:%s?cache=shared&mode=rwc",
		fmt.Sprintf("%s.sqlite", "nginx.err.intra"))
	dbIntra = sqldb.NewSqlDb(sqldb.DRIVER_SQLITE3, intraDsn, logger)
	dbIntra.CreateDb(`CREATE TABLE IF NOT EXISTS intra (host CHAR(30), ts INT, msg VARCHAR(200));`)

	extraDsn := fmt.Sprintf("file:%s?cache=shared&mode=rwc",
		fmt.Sprintf("%s.sqlite", "nginx.err.extra"))
	dbExtra = sqldb.NewSqlDb(sqldb.DRIVER_SQLITE3, extraDsn, logger)
	dbExtra.CreateDb(`CREATE TABLE IF NOT EXISTS extra (host CHAR(30), ts INT, msg VARCHAR(200));`)

	intraInsertStmt = dbIntra.Prepare("INSERT INTO intra(host, ts, msg) VALUES(?,?,?)")
	extraInsertStmt = dbExtra.Prepare("INSERT INTO extra(host, ts, msg) VALUES(?,?,?)")
}

func EachLogLine() chan []byte {
	bio := bufio.NewReader(os.Stdin)
	ch := make(chan []byte)
	go func() {
		for {
			line, err := readline(bio)
			if err != nil {
				fmt.Println(err)
				close(ch)
				break
			} else {
				ch <- line
			}
		}
	}()

	return ch
}

func readline(bio *bufio.Reader) ([]byte, error) {
	line, isPrefix, err := bio.ReadLine()
	if !isPrefix {
		return line, err
	}

	// line is too long, read till eol
	buf := append([]byte(nil), line...)
	for isPrefix && err == nil {
		line, isPrefix, err = bio.ReadLine()
		buf = append(buf, line...)
	}
	return buf, err
}
