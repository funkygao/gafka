package inflights

type Inflights interface {
	Land(cluster, topic, group, partition string)

	TakeOff(cluster, topic, group, partition string, offset int64) error
}

var Default Inflights
