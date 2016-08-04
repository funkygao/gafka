package mysql

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestAppId(t *testing.T) {
	m := mysqlStore{}
	assert.Equal(t, 65601907, m.app_id("app1"))
}

func TestTableName(t *testing.T) {
	m := mysqlStore{}
	assert.Equal(t, "app1_foobar_v1", m.table("app1.foobar.v1"))
	assert.Equal(t, "app1_foobar_v1_34", m.table("app1.foobar.v1.34"))
}
