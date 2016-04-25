package main

/*
   +-----------------+
   | offset          | 8B -+
   |-----------------|     |-12B LogOverhead
   | size            | 4B -+
   |-----------------|
   | crc32           | 4B -+
   |-----------------|     |
   | magic           | 1B -|
   |-----------------|     |
   | attribute       | 1B -|-14B
   |-----------------|     |
   | key size        | 4B -| -1 if key is null
   |-----------------|     |
   | key             |     |
   |-----------------|     |
   | value size      | 4B -+
   |-----------------|
   | palyload        | (size-14)B
   +-----------------+
*/
type Message struct {
}

type MessageAndOffset struct {
	nextOffset int64
	Message
}
