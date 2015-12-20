// +build fasthttp

package main

import (
	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
)

// /topics/:topic/:ver?key=mykey&async=1
func (this *Gateway) pubHandler(ctx *fasthttp.RequestCtx, params fasthttprouter.Params) {

}

// /raw/topics/:topic/:ver
func (this *Gateway) pubRawHandler(ctx *fasthttp.RequestCtx, params fasthttprouter.Params) {

}

func (this *Gateway) pubWsHandler(ctx *fasthttp.RequestCtx, params fasthttprouter.Params) {
	ctx.Error("not implemented", fasthttp.StatusBadRequest)
}
