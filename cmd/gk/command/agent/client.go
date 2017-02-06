package agent

import (
	"encoding/json"
	"fmt"

	"github.com/funkygao/gorequest"
)

func (a *Agent) ListMembers(port int) {
	_, body, _ := gorequest.New().Get(a.membersUri(port)).End()
	v := map[string]interface{}{}
	err := json.Unmarshal([]byte(body), &v)
	if err != nil {
		fmt.Println(err)
		return
	}
	b, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(string(b))
}
