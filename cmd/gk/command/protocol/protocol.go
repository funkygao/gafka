package protocol

type Protocol interface {
	Unmarshal(payload []byte) string
}

func New(prot string) Protocol {
	switch prot {
	case "ascii":
		return &ascii{}

	case "zk":
		return &zk{}

	case "kafka":
		return &kafka{}

	default:
		return nil
	}
}
