package sla

func ValidateGuardName(name string) bool {
	if name != SlaKeyRetryTopic && name != SlaKeyDeadLetterTopic {
		return false
	}

	return true
}
