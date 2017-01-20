package ctx

import (
	"errors"
	"strconv"
	"strings"
)

var errInvalidLoadAvg = errors.New("invalid load avg line")

func ExtractLoadAvg1m(line string) (float64, error) {
	parts := strings.Split(line, "load average:")
	if len(parts) < 2 {
		return 0, errInvalidLoadAvg
	}

	loadAvg := strings.TrimSpace(parts[1])
	avgs := strings.SplitN(loadAvg, ",", 3)
	loadAvg1m, _ := strconv.ParseFloat(strings.TrimSpace(avgs[0]), 64)
	return loadAvg1m, nil

}
