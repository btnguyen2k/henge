package henge

import (
	"reflect"
	"testing"

	_ "github.com/btnguyen2k/gocosmos"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
)

func TestNewSqlConnection(t *testing.T) {
	name := "TestNewSqlConnection"

	url := "db://invalid"
	tz := "UTC"
	drv := "invalid"
	timeout := 0
	var poolOpt *prom.SqlPoolOptions = nil
	if sqlc, err := NewSqlConnection(url, tz, drv, prom.FlavorDefault, timeout, poolOpt); err == nil || sqlc != nil {
		t.Fatalf("%s failed: expecting nil/error but received %#v/%s", name, sqlc, err)
	}

	url = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
	tz = "invalid"
	drv = "gocosmos"
	if sqlc, err := NewSqlConnection(url, tz, drv, prom.FlavorDefault, timeout, poolOpt); err != nil || sqlc == nil {
		t.Fatalf("%s failed: %#v/%s", name, sqlc, err)
	}
}

func Test_DefaultFilterGeneratorSql(t *testing.T) {
	name := "Test_DefaultFilterGeneratorSql"

	input := NewUniversalBo("myid", 1234)
	expected := map[string]interface{}{SqlColId: "myid"}
	if filter := defaultFilterGeneratorSql("", input); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}
	if filter := defaultFilterGeneratorSql("", *input); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}

	input2 := godal.NewGenericBo()
	input2.GboSetAttr(FieldId, "myid2")
	expected = map[string]interface{}{SqlColId: "myid2"}
	if filter := defaultFilterGeneratorSql("", input2); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}

	input3 := map[string]interface{}{"filter": "value"}
	expected = map[string]interface{}{"filter": "value"}
	if filter := defaultFilterGeneratorSql("", input3); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}
}
