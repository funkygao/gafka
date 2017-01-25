package agent

import (
	"fmt"

	"github.com/funkygao/gorequest"
)

func (a *Agent) ListMembers() {
	_, body, _ := gorequest.New().Get(fmt.Sprintf("localhost:%d", 10114+1)).End()
	fmt.Println(string(body))
}
