package registry

type Backend interface {
	Registered() (bool, error)

	Register() error

	Deregister() error

	// Name of the registry backend.
	Name() string
}

var Default Backend
