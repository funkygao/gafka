package inflights

type Inflights interface {
	Land(cluster, topic, group, partition string, offset int64) error

	LandX(cluster, topic, group, partition string, offset int64) ([]byte, error)

	TakeOff(cluster, topic, group, partition string, offset int64, msg []byte) error

	Init() error
	Stop() error
}

var Default Inflights
