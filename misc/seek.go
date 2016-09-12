package main

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strconv"
)

func swallow(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()

	if len(os.Args) == 1 {
		fmt.Printf("Usage: %s file offset [len]\n", os.Args[0])
		return
	}

	f, err := os.OpenFile(os.Args[1], os.O_RDONLY, 0600)
	swallow(err)
	defer f.Close()

	pos, err := strconv.ParseInt(os.Args[2], 10, 64)
	swallow(err)

	n, err := f.Seek(pos, os.SEEK_SET)
	swallow(err)

	if n != pos {
		fmt.Printf("bad seek. exp %v, got %v\n", pos, n)
	}

	sz := 2
	if len(os.Args) == 4 {
		n, err := strconv.Atoi(os.Args[3])
		swallow(err)

		sz = n
	}

	var buf = make([]byte, sz, sz)
	read, err := io.ReadAtLeast(f, buf, sz)
	swallow(err)

	if read != sz {
		fmt.Printf("read too few contents: exp %d got %d\n", sz, read)
	} else {
		fmt.Println(buf)
		fmt.Println(string(buf))
	}
}
