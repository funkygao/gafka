package anomaly

import (
	"testing"

	"github.com/funkygao/anomalyzer"
	"github.com/funkygao/assert"
)

func TestAnamolyPkg(t *testing.T) {
	conf := &anomalyzer.AnomalyzerConf{
		Sensitivity: 0.1,
		UpperBound:  5,
		LowerBound:  0,
		ActiveSize:  1,
		NSeasons:    4,
		Methods:     []string{"diff", "fence", "highrank", "lowrank", "magnitude"},
	}
	anomaly, err := anomalyzer.NewAnomalyzer(conf, nil)
	assert.Equal(t, nil, err)

	dataPoints := []float64{5, 3, 6, 7, 8, 3, 2, 9, 16}
	for _, d := range dataPoints {
		anomaly.Push(d)
	}

	for i := 0; i < 10; i++ {
		anomaly.Push(float64(i))
		t.Logf("%+v %d %+v", anomaly.Data, i, anomaly.Eval())
	}
}
