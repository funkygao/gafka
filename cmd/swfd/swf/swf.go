package swf

// Swf is the SimpleWorkFlow server.
type Swf struct {
	apiServer *apiServer
}

func New() *Swf {
	this := &Swf{}
	return this
}

func (this *Swf) ServeForever() {
	this.setupApis()

	select {}
}
