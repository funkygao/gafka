package protos

type ascii struct{}

func (a *ascii) Unmarshal(payload []byte) string {
	return string(payload)
}
