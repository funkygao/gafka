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

# 转账例子

Alice向位于另外DB的Bob转账10元钱

### Option1

    Begin
    update alice=alice-10
    if pub(msg) == ok
        COMMIT
    else
        ROLLBACK

what's the problem?

    网络上有ack的情况下，成功一定是成功的，但失败不一定真失败
    pub失败时，broker有可能已经拿到消息了，但ack时失败，这会造成Alice回滚(没扣钱)，但Bob得到10元


### Option2

    prepare = pub(msg)
    Begin
    update alice=alice-10
    COMMIT
    pub(prepare)

    如果pub(prepare)失败，那么broker里就会有prepare(状态不确定)的消息，broker应该定期到producer上询问
    该消息对应的事务到底是commit了还是rollback了。
    因此，producer要保存事务状态表

*/

package gateway

import (
	"net/http"

	"github.com/funkygao/httprouter"
)

//go:generate goannotation $GOFILE
// @rest POST /v1/xa/prepare/:topic/:ver
func (this *pubServer) xa_prepare(w http.ResponseWriter, r *http.Request, params httprouter.Params) {

}

// @rest POST /v1/xa/commit?id=xx
func (this *pubServer) xa_commit(w http.ResponseWriter, r *http.Request, params httprouter.Params) {

}

// @rest PUT /v1/xa/rollback?id=xx
func (this *pubServer) xa_rollback(w http.ResponseWriter, r *http.Request, params httprouter.Params) {

}
