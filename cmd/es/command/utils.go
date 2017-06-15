package command

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"github.com/funkygao/dbus/pkg/cluster"
	"github.com/funkygao/golib/version"
	"github.com/funkygao/gorequest"
)

func refreshScreen() {
	c := exec.Command("clear")
	c.Stdout = os.Stdout
	c.Run()
	c.Wait()
}

func swallow(err error) {
	if err != nil {
		panic(err)
	}
}

func callEsAPI(p cluster.Participant, api string, method string, body string) (string, []error) {
	r := gorequest.New()
	uri := fmt.Sprintf("%s/api/v1/%s", p.APIEndpoint(), api)
	switch strings.ToUpper(method) {
	case "PUT":
		r = r.Put(uri)
	case "POST":
		r = r.Post(uri)
	case "GET":
		r = r.Get(uri)
	}

	reply, replyBody, errs := r.
		Set("User-Agent", fmt.Sprintf("dbus-%s", version.Revision)).
		SendString(body).
		End()
	if reply.StatusCode != http.StatusOK {
		return "", []error{fmt.Errorf("status %d", reply.StatusCode)}
	}
	return replyBody, errs
}
