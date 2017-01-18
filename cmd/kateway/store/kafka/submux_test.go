package kafka

import (
	"fmt"
	"testing"

	"github.com/funkygao/kafka-cg/consumergroup"
)

func TestSubMux(t *testing.T) {
	mux := newSubMux()
	cg := &consumergroup.ConsumerGroup{}
	for i := 0; i < 5; i++ {
		mux.register(fmt.Sprintf("127.0.0.1:%d", 10001+i), cg)
	}
	t.Logf("%+v", mux)

	for i := 0; i < 5; i++ {
		r := mux.kill(fmt.Sprintf("127.0.0.1:%d", 10001+i))
		t.Logf("%+v", mux)
		if i == 4 && r != true {
			t.Fail()
		}
	}

}
