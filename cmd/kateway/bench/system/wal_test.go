package system

import (
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
)

func BenchmarkFileAppendWithoutFsync(b *testing.B) {
	line := strings.Repeat("X", 1023) + "\n"
	path := "_file"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	defer os.Remove(path)

	for i := 0; i < b.N; i++ {
		f.WriteString(line)
	}

	b.SetBytes(1024)
}

func BenchmarkFileAppendWithManualFsync(b *testing.B) {
	line := strings.Repeat("X", 1023) + "\n"
	path := "_file"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	defer os.Remove(path)

	for i := 0; i < b.N; i++ {
		f.WriteString(line)
		f.Sync()
	}

	b.SetBytes(1024)
}

func BenchmarkFileAppendOpenWithFsync(b *testing.B) {
	line := strings.Repeat("X", 1023) + "\n"
	path := "_file"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0600)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	defer os.Remove(path)

	for i := 0; i < b.N; i++ {
		f.WriteString(line)
	}

	b.SetBytes(1024)
}

func BenchmarkPageCacheSeek(b *testing.B) {
	path := "_file"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	defer os.Remove(path)

	buf := make([]byte, 100)
	sz := 1 << 20
	f.WriteString(strings.Repeat("X", sz)) // all contents inside OS page cache

	for i := 0; i < b.N; i++ {
		offset := rand.Int63n(int64(sz) - 100)
		f.Seek(offset, os.SEEK_SET)
		io.ReadAtLeast(f, buf[0:], 100)
	}

}
