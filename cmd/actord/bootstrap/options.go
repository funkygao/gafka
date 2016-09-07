package bootstrap

var Options struct {
	Zone             string
	ShowVersion      bool
	LogFile          string
	LogLevel         string
	LogRotateSize    int
	InfluxAddr       string
	InfluxDbname     string
	ListenAddr       string
	ManagerType      string
	HintedHandoffDir string
}
