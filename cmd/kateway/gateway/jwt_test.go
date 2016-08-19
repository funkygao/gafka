package gateway

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestJwtToken(t *testing.T) {
	token, err := jwtToken("appid", "secret")
	assert.Equal(t, nil, err)
	assert.Equal(t, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBpZCI6ImFwcGlkIn0.YaURi84SXE2SYFgVnJzN8MW5PdN2xgHRqdNzPF_-usY", token)

	appid, err := tokenDecode("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBpZCI6ImFwcGlkIn0.YaURi84SXE2SYFgVnJzN8MW5PdN2xgHRqdNzPF_-usY")
	assert.Equal(t, nil, err)
	assert.Equal(t, "exp", appid)
}
