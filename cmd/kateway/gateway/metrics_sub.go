package gateway

import (
	"encoding/json"
	"expvar"
	"sync"

	"github.com/funkygao/gafka/telementry"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

type subMetrics struct {
	gw *Gateway

	SubQps      metrics.Meter
	ClientError metrics.Meter

	expConsumeOk      *expvar.Int
	expActiveConns    *expvar.Int
	expActiveUpstream *expvar.Int

	// multi-tenant related
	ConsumeMap    map[string]metrics.Counter // i consume msgs of others
	consumeMapMu  sync.RWMutex
	ConsumedMap   map[string]metrics.Counter // my msgs are consumed by others
	consumedMapMu sync.RWMutex               // TODO who are consuming my msgs
}

func NewSubMetrics(gw *Gateway) *subMetrics {
	this := &subMetrics{
		gw:          gw,
		ConsumeMap:  make(map[string]metrics.Counter),
		ConsumedMap: make(map[string]metrics.Counter),
		SubQps:      metrics.NewRegisteredMeter("sub.qps", metrics.DefaultRegistry),
		ClientError: metrics.NewRegisteredMeter(("sub.clienterr"), metrics.DefaultRegistry),
	}

	if Options.DebugHttpAddr != "" {
		this.expConsumeOk = expvar.NewInt("ConsumeOk")
		this.expActiveConns = expvar.NewInt("ConsumeConns")       // TODO
		this.expActiveUpstream = expvar.NewInt("ConsumeUpstream") // TODO
	}

	return this
}

func (this *subMetrics) Key() string {
	return "sub"
}

func (this *subMetrics) Load() {
	b, err := this.gw.zkzone.LoadKatewayMetrics(this.gw.id, this.Key())
	if err != nil {
		log.Warn("load %s metrics: %v", this.Key(), err)
		return
	}

	data := make(map[string]map[string]int64)
	json.Unmarshal(b, &data)

	for k, v := range data["sub"] {
		if _, present := this.ConsumeMap[k]; !present {
			this.ConsumeMap[k] = metrics.NewRegisteredCounter(k+"sub.ok", metrics.DefaultRegistry)
		}
		this.ConsumeMap[k].Inc(v)
	}
	for k, v := range data["subd"] {
		if _, present := this.ConsumedMap[k]; !present {
			this.ConsumedMap[k] = metrics.NewRegisteredCounter(k+"subd.ok", metrics.DefaultRegistry)
		}
		this.ConsumedMap[k].Inc(v)
	}
}

func (this *subMetrics) Flush() {
	var data = make(map[string]map[string]int64)
	data["sub"] = make(map[string]int64)
	data["subd"] = make(map[string]int64)
	for k, v := range this.ConsumeMap {
		data["sub"][k] = v.Count()
	}
	for k, v := range this.ConsumedMap {
		data["subd"][k] = v.Count()
	}

	b, _ := json.Marshal(data)
	this.gw.zkzone.FlushKatewayMetrics(this.gw.id, this.Key(), b)
}

func (this *subMetrics) ConsumeOk(appid, topic, ver string) {
	if this.expConsumeOk != nil {
		this.expConsumeOk.Add(1)
	}
	telementry.UpdateCounter(appid, topic, ver, "sub.ok", 1, &this.consumeMapMu, this.ConsumeMap)
}

func (this *subMetrics) ConsumedOk(appid, topic, ver string) {
	telementry.UpdateCounter(appid, topic, ver, "subd.ok", 1, &this.consumedMapMu, this.ConsumedMap)
}
