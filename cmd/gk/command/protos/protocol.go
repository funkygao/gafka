package protos

type Protocol interface {
	Unmarshal(payload []byte) string
}

func New(prot string, serverPort int) Protocol {
	switch prot {
	case "ascii":
		return &ascii{}

	case "zk":
		return &zk{serverPort: serverPort}

	case "kafka":
		return &kafka{serverPort: serverPort}

	default:
		return nil
	}
}
