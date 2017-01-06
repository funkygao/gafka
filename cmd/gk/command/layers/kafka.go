package layers

import (
	"github.com/google/gopacket"
)

type KafkaLayer struct {
	data []byte

	ApiKey uint16
}

var LayerTypeKafka = gopacket.RegisterLayerType(
	9092,
	gopacket.LayerTypeMetadata{
		Name:    "Kafka",
		Decoder: gopacket.DecodeFunc(decodeKafkaLayer),
	},
)

func (l KafkaLayer) LayerType() gopacket.LayerType {
	return LayerTypeKafka
}

// TODO return request meta info.
func (l KafkaLayer) LayerContents() []byte {
	return l.data
}

// LayerPayload returns the subsequent layer built on top of our layer or raw payload.
func (l KafkaLayer) LayerPayload() []byte {
	return l.data
}

func decodeKafkaLayer(data []byte, p gopacket.PacketBuilder) error {
	p.AddLayer(&KafkaLayer{data: data})

	// The return value tells the packet what layer to expect
	// with the rest of the data. It could be another header layer,
	// nothing, or a payload layer.

	// nil means this is the last layer. No more decoding
	// return nil

	// Returning another layer type tells it to decode
	// the next layer with that layer's decoder function
	// return p.NextDecoder(layers.LayerTypeEthernet)

	// Returning payload type means the rest of the data
	// is raw payload. It will set the application layer
	// contents with the payload
	return p.NextDecoder(gopacket.LayerTypePayload)
}
