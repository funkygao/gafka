package gateway

import (
	"sync"
	"testing"
)

func BenchmarkConcurrentChannel(b *testing.B) {
	ch := make(chan struct{}, 500)
	go func() {
		for {
			<-ch
		}
	}()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch <- struct{}{}
		}
	})
}

func BenchmarkConcurrentMutex(b *testing.B) {
	var mu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			mu.Unlock()
		}
	})
}

func BenchmarkConcurrentRWMutex(b *testing.B) {
	var mu sync.RWMutex
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.RLock()
			mu.RUnlock()
		}
	})
}
