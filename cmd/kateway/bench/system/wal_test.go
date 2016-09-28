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

// dd if=/dev/zero of=_x bs=4096 count=2621440 # create a 10GB file beforehand
// 1693 ns/op
func BenchmarkPageCacheSeek(b *testing.B) {
	path := "_x"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	const bufSz = 1 << 10
	buf := make([]byte, bufSz)
	step := 10
	sz := step << 30

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		offset := rand.Int63n(int64(sz) - bufSz)
		f.Seek(offset, os.SEEK_SET)
		io.ReadAtLeast(f, buf[0:], bufSz)
	}

}
