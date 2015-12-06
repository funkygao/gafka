package registry

type Backend interface {
	// Register registers kateway as a service in the registry.
	Register() error

	// Deregister removes the service registration for kateway.
	Deregister() error
}

var Default Backend
