package command

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type ZkInstall struct {
	Ui  cli.Ui
	Cmd string

	singleMode bool
	rootPath   string
	myId       string
	servers    string
}

func (this *ZkInstall) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("zkinstall", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.rootPath, "root", "/var/wd/zookeeper", "")
	cmdFlags.StringVar(&this.myId, "id", "", "")
	cmdFlags.StringVar(&this.servers, "servers", "", "")
	cmdFlags.BoolVar(&this.singleMode, "single", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if !ctx.CurrentUserIsRoot() {
		this.Ui.Error("requires root priviledges!")
		return 1
	}

	if !this.singleMode {
		if validateArgs(this, this.Ui).
			require("-id", "-servers").
			invalid(args) {
			return 2
		}
	}

	// create dirs
	this.rootPath = strings.TrimSuffix(this.rootPath, "/")
	for _, d := range []string{"bin", "conf", "data", "lib", "log"} {
		swallow(os.MkdirAll(fmt.Sprintf("%s/%s", this.rootPath, d), 0755))
	}

	type templateVar struct {
		MyId     string
		RootPath string
		Servers  string
	}
	data := templateVar{
		MyId:     this.myId,
		RootPath: this.rootPath,
	}
	if !this.singleMode {
		servers := make([]string, 0)
		for _, s := range strings.Split(this.servers, ",") {
			parts := strings.SplitN(s, ":", 2)
			servers = append(servers, fmt.Sprintf("server.%s=%s:2888:3888",
				parts[0], parts[1]))
		}
		data.Servers = strings.Join(servers, "\n")
	}

	// copy all files in bin and lib
	for srcDir, dstDir := range map[string]string{
		"template/zk/bin": fmt.Sprintf("%s/bin", this.rootPath),
		"template/zk/lib": fmt.Sprintf("%s/lib", this.rootPath)} {
		files, err := AssetDir(srcDir)
		swallow(err)
		for _, srcFile := range files {
			_, dstFile := path.Split(srcFile)
			from := fmt.Sprintf("%s/%s", srcDir, srcFile)
			to := fmt.Sprintf("%s/%s", dstDir, dstFile)
			var perm os.FileMode = 0644
			if strings.HasSuffix(srcDir, "/bin") {
				perm = 0755
			}
			writeFileFromTemplate(from, to, perm, nil, nil)
		}
	}

	// zk jar
	writeFileFromTemplate("template/zk/zookeeper-3.4.6.jar",
		fmt.Sprintf("%s/zookeeper-3.4.6.jar", this.rootPath), 0644, nil, nil)

	// tempated conf
	writeFileFromTemplate("template/zk/conf/zoo.cfg",
		fmt.Sprintf("%s/conf/zoo.cfg", this.rootPath), 0644, data, nil)
	writeFileFromTemplate("template/zk/conf/log4j.properties",
		fmt.Sprintf("%s/conf/log4j.properties", this.rootPath), 0644, nil, nil)

	// templated data/myid
	if !this.singleMode {
		writeFileFromTemplate("template/zk/data/myid",
			fmt.Sprintf("%s/data/myid", this.rootPath), 0644, data, nil)
	}

	// templated init.d/
	writeFileFromTemplate("template/init.d/zookeeper",
		"/etc/init.d/zookeeper", 0755, data, nil)

	this.Ui.Info("zookeeper installed on localhost")
	this.Ui.Warn(fmt.Sprintf("NOW, please run the following command:"))
	this.Ui.Warn("yum install -y jdk-1.7.0_65-fcs.x86_64")
	this.Ui.Output(color.Red("chkconfig --add zookeeper"))
	this.Ui.Output(color.Red("/etc/init.d/zookeeper start"))

	return
}

func (*ZkInstall) Synopsis() string {
	return "Install a zookeeper node on localhost"
}

func (this *ZkInstall) Help() string {
	help := fmt.Sprintf(`
Usage: %s zkinstall [options]

    %s

Options:    
   
    -id id
      myid of this zookeeper node
      id start from 1 instead of 0

    -servers comma seperated ip addrs
      e,g. 1:10.213.1.225,2:10.213.10.140,3:10.213.18.207

    -single
      install as single node mode
    
`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
