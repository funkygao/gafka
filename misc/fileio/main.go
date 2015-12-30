package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"syscall"
	"time"
)

var (
	fileName = flag.String("fname", "/tmp/big.1gb", "file name for testing.")
	fileSize = flag.Int("fsize", 1024*1024*1024, "file size.")
	recSize  = flag.Int("rsize", 1024, "record size.")
	numIO    = flag.Int("n", 10000, "number of io operations.")
	write    = flag.Bool("write", false, "file name for testing.")
	read     = flag.Bool("read", false, "sequential read from file with os.File.Read.")
	mread    = flag.Bool("mread", false, "sequential read from file with mmap and copy")
	pread    = flag.Bool("pread", false, "random read with os.File.ReadAt.")
	mmap     = flag.Bool("mmap", false, "random read from mmaped file.")
)

// randomReadMmap does n random reads of records of size rsize using mmap.
func randomReadMmap(fname string, rsize int, n int) error {
	rand.Seed(time.Now().UnixNano())
	f, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	fsize := int(fi.Size())
	data, err := syscall.Mmap(int(f.Fd()), 0, fsize, syscall.PROT_READ, syscall.MAP_FILE|syscall.MAP_SHARED)
	defer syscall.Munmap(data)
	buf := make([]byte, rsize)
	for i := 0; i < n; i++ {
		offset := rand.Intn(fsize - rsize)
		copy(buf, data[offset:offset+len(buf)])
	}
	return nil
}

// randomReadPread does n random reads of records of size rsize using pread (os.File.ReadAt).
func randomReadPread(fname string, rsize int, n int) error {
	rand.Seed(time.Now().UnixNano())
	f, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	fsize := int(fi.Size())
	buf := make([]byte, rsize)
	for i := 0; i < n; i++ {
		offset := rand.Intn(fsize - rsize)
		_, err := f.ReadAt(buf, int64(offset))
		if err != nil && err != io.EOF {
			return err
		}
	}
	return nil
}

// seqRead does n sequential reads of records of size rsize using read (os.File.Read).
func seqRead(fname string, rsize int, n int) (int, error) {
	f, err := os.Open(fname)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	fsize := int(fi.Size())
	buf := make([]byte, rsize)
	if n <= 0 {
		n = fsize / rsize
	}
	var i int
	for i = 0; i < n; i++ {
		_, err := f.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return i, err
			}
		}
	}
	return i, nil
}

// seqMmapRead does n sequential reads of records of size rsize using mmap and copy.
func seqMmapRead(fname string, rsize int, n int) (int, error) {
	f, err := os.Open(fname)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	fsize := int(fi.Size())
	data, err := syscall.Mmap(int(f.Fd()), 0, fsize, syscall.PROT_READ, syscall.MAP_FILE|syscall.MAP_SHARED)
	// syscall.Madvise(data, syscall.MADV_SEQUENTIAL)
	defer syscall.Munmap(data)
	buf := make([]byte, rsize)
	if n <= 0 || n > fsize/rsize {
		n = fsize / rsize
	}
	var i int
	for i = 0; i < n; i++ {
		offset := i * rsize
		copy(buf, data[offset:offset+len(buf)])
	}
	return i, nil
}

// writeFile writes files of size - fsize.
func writeFile(fname string, rsize, fsize int) (int, error) {
	f, err := os.Create(fname)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	buf := make([]byte, rsize)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i % 256)
	}
	n := fsize / rsize
	for i := 0; i < n; i++ {
		if _, err := f.Write(buf); err != nil {
			return n, nil
		}
	}
	return n, nil
}

func throughput(rsize, num int, d time.Duration) float64 {
	mb := float64(rsize*num) / (1024 * 1024)
	sec := float64(d) / float64(time.Second)
	return mb / sec
}

func main() {
	flag.Parse()
	if !*write && !*read && !*mread && !*pread && !*mmap {
		fmt.Printf("Must choose at least one action -  write, read, pread, or mmap!\n")
		flag.PrintDefaults()
	}

	if *write {
		lapse := time.Now()
		n, err := writeFile(*fileName, *recSize, *fileSize)
		if err != nil {
			log.Fatalf("writeFile: %v", err)
		}
		d := time.Now().Sub(lapse)
		fmt.Printf("%8s - sequential writes for record: %6d bytes (n: %d), throughput: %.2fMB/sec\n", d, *recSize, n, throughput(*recSize, n, d))
	}
	if *read {
		lapse := time.Now()
		n, err := seqRead(*fileName, *recSize, -1)
		if err != nil {
			log.Fatalf("seqRead: %v", err)
		}
		d := time.Now().Sub(lapse)
		fmt.Printf("%8s - sequential reads for record: %6d bytes (n: %d), throughput: %.2fMB/sec\n", d, *recSize, n, throughput(*recSize, n, d))
	}
	if *mread {
		lapse := time.Now()
		n, err := seqMmapRead(*fileName, *recSize, -1)
		if err != nil {
			log.Fatalf("seqMapRead: %v", err)
		}
		d := time.Now().Sub(lapse)
		fmt.Printf("%8s - sequential reads for record: %6d bytes (n: %d), throughput: %.2fMB/sec\n", d, *recSize, n, throughput(*recSize, n, d))
	}

	if *pread {
		lapse := time.Now()
		if err := randomReadPread(*fileName, *recSize, *numIO); err != nil {
			log.Fatalf("randomReadPread: %v", err)
		}
		d := time.Now().Sub(lapse)
		fmt.Printf("%8s - random reads with pread for record: %6d bytes (n: %d) throughput: %.2fMB/sec \n", d, *recSize, *numIO, throughput(*recSize, *numIO, d))
	}

	if *mmap {
		lapse := time.Now()
		if err := randomReadMmap(*fileName, *recSize, *numIO); err != nil {
			log.Fatalf("randomReadMmap: %v", err)
		}
		d := time.Now().Sub(lapse)
		fmt.Printf("%8s - random reads with mmap  for record: %6d bytes (n: %d) throughput: %.2fMB/sec \n", d, *recSize, *numIO, throughput(*recSize, *numIO, d))
	}
}
