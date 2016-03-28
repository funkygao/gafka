package sla

func ValidateShadowName(name string) bool {
	if name != SlaKeyRetryTopic && name != SlaKeyDeadLetterTopic {
		return false
	}

	return true
}
