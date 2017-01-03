package protocol

type zk struct{}

func (z *zk) Unmarshal(payload []byte) string {
	return ""
}
