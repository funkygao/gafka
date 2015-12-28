package registry

type Backend interface {
	Register() error

	Deregister() error
}

var Default Backend
