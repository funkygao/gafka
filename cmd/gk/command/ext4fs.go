package command

import (
	"bufio"
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/pipestream"
)

type Ext4fs struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Ext4fs) Run(args []string) (exitCode int) {
	var noJournal string
	cmdFlags := flag.NewFlagSet("ext4fs", flag.ContinueOnError)
	cmdFlags.StringVar(&noJournal, "nojournal", "", "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if noJournal {
		this.genNoJournalScript(noJournal)
		return
	}

	this.Ui.Output(fmt.Sprintf(`
dumpe2fs /dev/XXX | grep 'Filesystem features' | grep 'has_journal'

mount -t ext4 -o remount,rw,barrier=0,commit=60,noatime,nodiratime  /dev/XXX /dataYYY

umount /dataYYY
tune2fs -O ^has_journal /dev/XXX
fsck.ext4 -f /dev/XXX
mount -t ext4 -o rw,barrier=0,commit=60,noatime,nodiratime /dev/XXX /dataYYY
		`))

	return
}

func (this *Ext4fs) genNoJournalScript(mountPointPattern string) {
	cmd := pipestream.New("df")
	err := cmd.Open()
	swallow(err)
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)

	fileSystems := make(map[string]string) // dev:mountPoint
	for scanner.Scan() {
		tuples := strings.Fields(scanner.Text())
		if !patternMatched(tuples[5], mountPointPattern) {
			continue
		}

		fileSystems[tuples[0]] = tuples[5]
	}
	swallow(scanner.Err())

	for dev, mp := range fileSystems {
		this.Ui.Output(fmt.Sprintf(`umount %s
tune2fs -O ^has_journal %s
fsck.ext4 -f %s`, mp, dev, dev))
	}

}

func (*Ext4fs) Synopsis() string {
	return "Tune ext4 file system for localhost"
}

func (this *Ext4fs) Help() string {
	help := fmt.Sprintf(`
Usage: %s ext4 [options]

    %s

Options:

    -nojournal mount point pattern
      Generate script to turn off journal feature for ext4

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
