package kafka

import (
	"github.com/Shopify/sarama"
)

var (
	ErrEmptyBrokerList = sarama.ConfigurationError("You must provide at least one broker address")
)
