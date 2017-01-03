package protocol

type kafka struct{}

func (k *kafka) Unmarshal(payload []byte) string {
	return ""
}
