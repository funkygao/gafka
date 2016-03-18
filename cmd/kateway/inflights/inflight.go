package inflights

type Inflights interface {
	Land(cluster, topic, group, partition string, offset int64) error

	TakeOff(cluster, topic, group, partition string, offset int64) error

	// TakenOff judges whether an offset has taken off but not landed.
	TakenOff(cluster, topic, group, partition string, offset int64) bool

	Init() error
	Stop() error
}

var Default Inflights
