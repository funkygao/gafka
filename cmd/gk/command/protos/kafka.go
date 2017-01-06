package protos

import (
	"encoding/binary"
	"fmt"
)

type kafka struct {
	serverPort int
}

func (k *kafka) Unmarshal(srcPort, dstPort uint16, payload []byte) string {
	if dstPort == uint16(k.serverPort) {
		return k.unmarshalRequest(payload)
	}

	return k.unmarshalResponse(payload)
}

func (k *kafka) unmarshalRequest(payload []byte) string {
	// request header
	//===============
	// 0-3 message_size
	// 4-5 api_key
	// 6-7 api_version
	// 8-11 correlation_id
	// 12-13 client_id length

	apiKey := binary.BigEndian.Uint16(payload[4:6])
	switch apiKey {
	case 0: // Produce
	case 1: // Fetch
		apiVersion := binary.BigEndian.Uint16(payload[6:8]) // 0 1 2

		clientIDLen := binary.BigEndian.Uint16(payload[12:14])
		clientID := string(payload[14 : 14+int(clientIDLen)])

		offset := int(clientIDLen + 14)
		// replica:4 max_wait:4 min_bytes:4 topicsN:4
		offset += 4 * 4
		topicLen := binary.BigEndian.Uint16(payload[offset : offset+2])
		offset += 2
		topic := string(payload[offset : offset+int(topicLen)])
		offset += int(topicLen)
		partitionN := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		partition := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4
		msgOffset := binary.BigEndian.Uint64(payload[offset : offset+8])
		offset += 8
		maxBytes := binary.BigEndian.Uint32(payload[offset : offset+4])
		return fmt.Sprintf("Fetch-%d[%s/%2d#%d] @%-11d max:%-8d %s",
			apiVersion, topic, partition, partitionN, msgOffset, maxBytes, clientID)

	case 2: // Offsets
	case 3: // Metadata
	case 4: // LeaderAndIsr
	case 5: // StopReplica
	case 6: // UpdateMetadata
	case 7: // ControlledShutdown
	case 8: // OffsetCommit
	case 9: // OffsetFetch
	case 19: // CreateTopics
	case 20: // DeleteTopics
	}

	return ""
}

func (k *kafka) unmarshalResponse(payload []byte) string {
	// response header
	//================
	// 0-3 message_size
	// 4-7 correlation_id

	return ""
}
