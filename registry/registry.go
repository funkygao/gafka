package registry

type Backend interface {
	Register() error

	Deregister() error

	// Name of the registry backend.
	Name() string
}

var Default Backend
