package main

import (
	"fmt"
	"os"
	"strconv"
)

func swallow(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	if len(os.Args) == 1 {
		fmt.Printf("Usage: %s file offset\n", os.Args[0])
		return
	}

	f, err := os.OpenFile(os.Args[1], os.O_RDONLY, 0600)
	swallow(err)

	pos, err := strconv.ParseInt(os.Args[2], 10, 64)
	swallow(err)

	n, err := f.Seek(pos, os.SEEK_SET)
	swallow(err)

	if n != pos {
		fmt.Printf("bad seek. exp %v, got %v\n", pos, n)
	}

	var buf [4]byte
	read, err := f.Read(buf[:2])
	swallow(err)

	if read != 2 {
		fmt.Println("read too few contents")
	} else {
		fmt.Println(buf)
	}
}
