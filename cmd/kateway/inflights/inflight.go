package inflights

type Inflights interface {
	Land(topic, ver, group, partition string)

	TakeOff(topic, ver, group, partition string, offset int64) error
}

var Default Inflights
