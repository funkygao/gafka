package mysql

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestAppId(t *testing.T) {
	assert.Equal(t, 65601907, App_id("app1"))
}

func TestJobTable(t *testing.T) {
	assert.Equal(t, "app1_foobar_v1", JobTable("app1.foobar.v1"))
	assert.Equal(t, "app1_foobar_v1_34", JobTable("app1.foobar.v1.34"))
	assert.Equal(t, "app1_foobar_v1_34_archive", HistoryTable("app1.foobar.v1.34"))
}
