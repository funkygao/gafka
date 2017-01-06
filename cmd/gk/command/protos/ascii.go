package protos

type ascii struct{}

func (a *ascii) Unmarshal(srcPort, dstPort uint16, payload []byte) string {
	return string(payload)
}
