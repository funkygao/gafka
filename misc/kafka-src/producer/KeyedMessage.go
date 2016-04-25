package main

type KeyedMessage struct {
	topic   string
	key     []byte
	message []byte
}
