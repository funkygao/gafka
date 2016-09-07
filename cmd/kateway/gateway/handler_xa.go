/*
In XA protocol, kateway acts as a RM.

XA usage example
================

id = xa_prepare(msg)
redo_log(id).set(prepare)
try {
	begin
	update account set balance = balance - 10
	commit
	redo_log(id).set(ok)

	xa_commit(id)
} catch {
	redo_log(id).set(rollback)
	rollback

	xa_rollback(id)
}
*/

package gateway

import (
	"net/http"

	"github.com/funkygao/httprouter"
)

// POST /v1/xa/prepare/:topic/:ver
func (this *pubServer) xa_prepare(w http.ResponseWriter, r *http.Request, params httprouter.Params) {

}

// /v1/xa/commit?id=xx
func (this *pubServer) xa_commit(w http.ResponseWriter, r *http.Request, params httprouter.Params) {

}

// PUT /v1/xa/rollback?id=xx
func (this *pubServer) xa_rollback(w http.ResponseWriter, r *http.Request, params httprouter.Params) {

}
