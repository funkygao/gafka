package monitor

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

// POST /alertHook
// so that we can auto-fix
func (this *Monitor) alertHookHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {

}
