// +build !windows

package console

import (
	"fmt"
	"sort"
	"time"

	"github.com/funkygao/go-metrics"
	w "github.com/funkygao/log4go"
)

func runMetricsReporter(r metrics.Registry, d time.Duration) {
	for range time.Tick(d) {
		sortedNames := make([]string, 0, 1000)
		metricsMap := make(map[string]interface{}, 1000)
		r.Each(func(name string, i interface{}) {
			metricsMap[name] = i
			sortedNames = append(sortedNames, name)
		})

		sort.Strings(sortedNames)
		for _, name := range sortedNames {
			switch metric := metricsMap[name].(type) {
			case metrics.Counter:
				w.Info(fmt.Sprintf("cnter %s: count: %d", name, metric.Count()))

			case metrics.Gauge:
				w.Info(fmt.Sprintf("gauge %s: value: %d", name, metric.Value()))

			case metrics.GaugeFloat64:
				w.Info(fmt.Sprintf("gauge %s: value: %f", name, metric.Value()))

			case metrics.Healthcheck:
				metric.Check()
				w.Info(fmt.Sprintf("hthck %s: error: %v", name, metric.Error()))

			case metrics.Histogram:
				h := metric.Snapshot()
				ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
				w.Info(fmt.Sprintf("histg %s: count: %d min: %d max: %d mean: %.2f stddev: %.2f median: %.2f 75%%: %.2f 95%%: %.2f 99%%: %.2f 99.9%%: %.2f",
					name,
					h.Count(),
					h.Min(),
					h.Max(),
					h.Mean(),
					h.StdDev(),
					ps[0],
					ps[1],
					ps[2],
					ps[3],
					ps[4],
				))

			case metrics.Meter:
				m := metric.Snapshot()
				w.Info(fmt.Sprintf("meter %s: count: %d 1-min: %.2f 5-min: %.2f 15-min: %.2f mean: %.2f",
					name,
					m.Count(),
					m.Rate1(),
					m.Rate5(),
					m.Rate15(),
					m.RateMean(),
				))

			case metrics.Timer:
				t := metric.Snapshot()
				ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
				w.Info(fmt.Sprintf("timer %s: count: %d min: %d max: %d mean: %.2f stddev: %.2f median: %.2f 75%%: %.2f 95%%: %.2f 99%%: %.2f 99.9%%: %.2f 1-min: %.2f 5-min: %.2f 15-min: %.2f mean-rate: %.2f",
					name,
					t.Count(),
					t.Min(),
					t.Max(),
					t.Mean(),
					t.StdDev(),
					ps[0],
					ps[1],
					ps[2],
					ps[3],
					ps[4],
					t.Rate1(),
					t.Rate5(),
					t.Rate15(),
					t.RateMean(),
				))
			}
		}

	}
}
