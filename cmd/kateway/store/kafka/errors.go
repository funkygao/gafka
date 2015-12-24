package kafka

import (
	"github.com/Shopify/sarama"
)

var (
	ErrorKafkaConfig = sarama.ConfigurationError("You must provide at least one broker address")
)
