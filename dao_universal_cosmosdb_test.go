package henge

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/checksum"
	"github.com/btnguyen2k/consu/reddo"
	_ "github.com/btnguyen2k/gocosmos"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
)

func TestRowMapperCosmosdb_ToRow(t *testing.T) {
	name := "TestRowMapperCosmosdb_ToRow"
	rm := buildRowMapperCosmosdb()
	gbo := godal.NewGenericBo()
	gbo.GboSetAttr(FieldTagVersion, 123)
	now := time.Now().Round(time.Millisecond)
	gbo.GboSetAttr(FieldTimeCreated, now)
	gbo.GboSetAttr(FieldTimeUpdated, now)
	gbo.GboSetAttr(FieldData, `{"field":"value"}`)
	row, err := rm.ToRow("tbl_test", gbo)
	if err != nil || row == nil {
		t.Fatalf("%s failed: %s / %#v", name, err, row)
	}
	// row should be map[string]interface{}
	rowMap, ok := row.(map[string]interface{})
	if !ok || rowMap == nil {
		t.Fatalf("%s failed: expect row to be map[string]interface{} but received %#v", name, rowMap)
	}
	if v, err := reddo.ToInt(rowMap[FieldTagVersion]); err != nil || v != 123 {
		t.Fatalf("%s failed: expect row[%s] to be %#v but received %#v/%s", name, FieldTagVersion, 123, v, err)
	}
	if v, err := reddo.ToTimeWithLayout(rowMap[FieldTimeCreated], time.RFC3339Nano); err != nil || !v.Equal(now) {
		t.Fatalf("%s failed: expect row[%s] to be %#v but received %#v/%s", name, FieldTimeCreated, now, v, err)
	}
	if v, err := reddo.ToTimeWithLayout(rowMap[FieldTimeUpdated], time.RFC3339Nano); err != nil || !v.Equal(now) {
		t.Fatalf("%s failed: expect row[%s] to be %#v but received %#v/%s", name, FieldTimeUpdated, now, v, err)
	}
	dataMap := map[string]interface{}{"field": "value"}
	if v, err := reddo.ToMap(rowMap[FieldData], reflect.TypeOf(dataMap)); err != nil || !reflect.DeepEqual(v, dataMap) {
		t.Fatalf("%s failed: expect row[%s] to be %#v but received %#v/%s", name, FieldData, dataMap, v, err)
	}
}

func TestRowMapperCosmosdb_ToBo(t *testing.T) {
	name := "TestRowMapperCosmosdb_ToBo"
	rm := buildRowMapperCosmosdb()
	if bo, err := rm.ToBo("tbl_test", map[string]interface{}{FieldData: `{"field":"value"}`}); err != nil || bo == nil {
		t.Fatalf("%s failed: %s / %#v", name, err, bo)
	} else if data, err := bo.GboGetAttr(FieldData, nil); err != nil || data != `{"field":"value"}` {
		t.Fatalf("%s failed: %s / %#v", name, err, data)
	}
	if bo, err := rm.ToBo("tbl_test", map[string]interface{}{FieldData: []byte(`{"field":"value"}`)}); err != nil || bo == nil {
		t.Fatalf("%s failed: %s / %#v", name, err, bo)
	} else if data, err := bo.GboGetAttr(FieldData, nil); err != nil || data != `{"field":"value"}` {
		t.Fatalf("%s failed: %s / %#v", name, err, data)
	}
	if bo, err := rm.ToBo("tbl_test", map[string]interface{}{FieldData: map[string]string{"field": "value"}}); err != nil || bo == nil {
		t.Fatalf("%s failed: %s / %#v", name, err, bo)
	} else if data, err := bo.GboGetAttr(FieldData, nil); err != nil || data != `{"field":"value"}` {
		t.Fatalf("%s failed: %s / %#v", name, err, data)
	}
}

func Test_CosmosdbFilterGeneratorSql(t *testing.T) {
	name := "Test_CosmosdbFilterGeneratorSql"
	var expected godal.FilterOpt

	input := NewUniversalBo("myid", 1234)
	expected = &godal.FilterOptFieldOpValue{FieldName: CosmosdbColId, Operator: godal.FilterOpEqual, Value: "myid"}
	if filter := cosmosdbFilterGeneratorSql("", input); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}
	if filter := cosmosdbFilterGeneratorSql("", *input); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}

	input2 := godal.NewGenericBo()
	input2.GboSetAttr(FieldId, "myid2")
	expected = &godal.FilterOptFieldOpValue{FieldName: CosmosdbColId, Operator: godal.FilterOpEqual, Value: "myid2"}
	if filter := cosmosdbFilterGeneratorSql("", input2); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}

	input3 := godal.MakeFilter(map[string]interface{}{CosmosdbColId: "myid3"})
	expected = &godal.FilterOptFieldOpValue{FieldName: CosmosdbColId, Operator: godal.FilterOpEqual, Value: "myid3"}
	if filter := cosmosdbFilterGeneratorSql("", input3); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}
}

func _cleanupCosmosdb(sqlc *prom.SqlConnect, tableName string) error {
	_, err := sqlc.GetDB().Exec(fmt.Sprintf("DROP COLLECTION IF EXISTS %s", tableName))
	return err
}

func _testCosmosdbInitSqlConnect(t *testing.T, testName, tableName string) *prom.SqlConnect {
	driver := strings.ReplaceAll(os.Getenv("COSMOSDB_DRIVER"), `"`, "")
	url := strings.ReplaceAll(os.Getenv("COSMOSDB_URL"), `"`, "")
	if driver == "" || url == "" {
		t.Skipf("%s skipped", testName)
		return nil
	}
	timezone := strings.ReplaceAll(os.Getenv("TIMEZONE"), `"`, "")
	if timezone == "" {
		timezone = "UTC"
	}
	urlTimezone := strings.ReplaceAll(timezone, "/", "%2f")
	url = strings.ReplaceAll(url, "${loc}", urlTimezone)
	url = strings.ReplaceAll(url, "${tz}", urlTimezone)
	url = strings.ReplaceAll(url, "${timezone}", urlTimezone)
	url += ";Db=henge"
	sqlc, err := NewCosmosdbConnection(url, timezone, driver, 10000, nil)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewCosmosdbConnection", err)
	}
	sqlc.GetDB().Exec("CREATE DATABASE henge WITH maxru=10000")
	if err := _cleanupCosmosdb(sqlc, tableName); err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "_cleanupCosmosdb", err)
	}
	return sqlc
}

func TestInitCosmosdbCollection(t *testing.T) {
	name := "TestInitCosmosdbCollection"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()

	_cleanupCosmosdb(sqlc, tblName)
	if err := InitCosmosdbCollection(sqlc, tblName, &CosmosdbCollectionSpec{Pk: "pk", Ru: 400}); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	_cleanupCosmosdb(sqlc, tblName)
	if err := InitCosmosdbCollection(sqlc, tblName, &CosmosdbCollectionSpec{LargePk: "largepk", MaxRu: 4000}); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	_cleanupCosmosdb(sqlc, tblName)
	if err := InitCosmosdbCollection(sqlc, tblName, &CosmosdbCollectionSpec{Pk: "pk", Uk: [][]string{{"/uk1"}, {"/uk2a", "/uk2b"}}}); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
}

func TestNewCosmosdbConnection(t *testing.T) {
	name := "TestNewCosmosdbConnection"
	sqlc := _testCosmosdbInitSqlConnect(t, name, "table_temp")
	defer sqlc.Close()
}

func TestInitCosmosdbTable(t *testing.T) {
	name := "TestInitCosmosdbTable"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for i := 0; i < 2; i++ {
		spec := &CosmosdbCollectionSpec{Pk: "id"}
		if err := InitCosmosdbCollection(sqlc, tblName, spec); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}
}

const (
	colCosmosdbPk = "type"
)

func _testCosmosdbInit(t *testing.T, name string, sqlc *prom.SqlConnect, tblName, pkName, pkValue string) UniversalDao {
	collectionSpec := &CosmosdbCollectionSpec{Pk: pkName, Uk: [][]string{{"/email"}}}
	if err := InitCosmosdbCollection(sqlc, tblName, collectionSpec); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	daoSpec := &CosmosdbDaoSpec{PkName: pkName, PkValue: pkValue, TxModeOnWrite: true}
	dao := NewUniversalDaoCosmosdbSql(sqlc, tblName, daoSpec)
	return dao
}

func TestNewUniversalDaoCosmosdbSql(t *testing.T) {
	name := "TestNewUniversalDaoCosmosdbSql"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")
	if cdao, _ := dao.(*UniversalDaoCosmosdbSql); cdao == nil {
		t.Fatalf("%s failed: not *UniversalDaoCosmosdbSql", name)
	} else if v := cdao.GetPkName(); v != colCosmosdbPk {
		t.Fatalf("%s failed: expected %#v but received %#v", name, colCosmosdbPk, v)
	} else if v := cdao.GetPkValue(); v != "users" {
		t.Fatalf("%s failed: expected %#v but received %#v", name, "users", v)
	}
}

func TestNewUniversalDaoCosmosdbSql_nil(t *testing.T) {
	name := "TestNewUniversalDaoCosmosdbSql_nil"
	dao := NewUniversalDaoCosmosdbSql(nil, "", nil)
	if dao != nil {
		t.Fatalf("%s failed: expected nil", name)
	}
}

func TestCosmosdb_Create(t *testing.T) {
	name := "TestCosmosdb_Create"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")
	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(colCosmosdbPk, "users")
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := dao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", name)
	}
}

func TestCosmosdb_CreateExistingPK(t *testing.T) {
	name := "TestCosmosdb_CreateExistingPK"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")
	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(colCosmosdbPk, "users")
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := dao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", name)
	}

	ubo.SetExtraAttr("email", "myname2@mydomain.com")
	if ok, err := dao.Create(ubo); err != godal.ErrGdaoDuplicatedEntry {
		t.Fatalf("%s failed: %s", name, err)
	} else if ok {
		t.Fatalf("%s failed: record should not be created twice", name)
	}
}

func TestCosmosdb_CreateExistingUnique(t *testing.T) {
	name := "TestCosmosdb_CreateExistingUnique"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")
	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(colCosmosdbPk, "users")
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := dao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", name)
	}

	ubo.SetId("id2")
	if ok, err := dao.Create(ubo); err != godal.ErrGdaoDuplicatedEntry {
		t.Fatalf("%s failed: %s", name, err)
	} else if ok {
		t.Fatalf("%s failed: record should not be created twice", name)
	}
}

func TestCosmosdb_CreateGet(t *testing.T) {
	name := "TestCosmosdb_CreateGet"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")
	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(colCosmosdbPk, "users")
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := dao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", name)
	}

	if bo, err := dao.Get("id"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", name)
	} else {
		if v := bo.GetTagVersion(); v != uint64(1357) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, uint64(1357), v)
		}
		if v := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh", v)
		}
		if v := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "myname@mydomain.com", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, int64(35), v)
		}
	}
}

func TestCosmosdb_CreateDelete(t *testing.T) {
	name := "TestCosmosdb_CreateDelete"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")
	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(colCosmosdbPk, "users")
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := dao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", name)
	}

	if ok, err := dao.Delete(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot delete record", name)
	}

	if bo, err := dao.Get("id"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if bo != nil {
		t.Fatalf("%s failed: record should be deleted", name)
	}

	if ok, err := dao.Delete(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if ok {
		t.Fatalf("%s failed: record should not be deleted twice", name)
	}
}

func TestCosmosdb_CreateGetMany(t *testing.T) {
	name := "TestCosmosdb_CreateGetMany"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
		ubo.SetExtraAttr(colCosmosdbPk, "users")
		ubo.SetDataAttr("name.first", strconv.Itoa(i))
		ubo.SetDataAttr("name.last", "Nguyen")
		ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
		ubo.SetExtraAttr("age", 35+i)
		if ok, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", name)
		}
	}

	if boList, err := dao.GetAll(nil, nil); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(boList) != 10 {
		t.Fatalf("%s failed: expected %#v items but received %#v", name, 10, len(boList))
	}
}

func TestCosmosdb_CreateGetManyWithFilter(t *testing.T) {
	name := "TestCosmosdb_CreateGetManyWithFilter"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
		ubo.SetExtraAttr(colCosmosdbPk, "users")
		ubo.SetDataAttr("name.first", strconv.Itoa(i))
		ubo.SetDataAttr("name.last", "Nguyen")
		ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
		ubo.SetExtraAttr("age", 35+i)
		if ok, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", name)
		}
	}

	filter := &godal.FilterOptFieldOpValue{FieldName: "age", Operator: godal.FilterOpGreaterOrEqual, Value: 35 + 3}
	if boList, err := dao.GetAll(filter, nil); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(boList) != 7 {
		t.Fatalf("%s failed: expected %#v items but received %#v", name, 7, len(boList))
	}
}

func TestCosmosdb_CreateGetManyWithSorting(t *testing.T) {
	name := "TestCosmosdb_CreateGetManyWithSorting"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
		ubo.SetExtraAttr(colCosmosdbPk, "users")
		ubo.SetDataAttr("name.first", strconv.Itoa(i))
		ubo.SetDataAttr("name.last", "Nguyen")
		ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
		ubo.SetExtraAttr("age", 35+i)
		if ok, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", name)
		}
	}

	sorting := (&godal.SortingField{FieldName: "email", Descending: true}).ToSortingOpt()
	if boList, err := dao.GetAll(nil, sorting); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else {
		for i := 0; i < 10; i++ {
			if boList[i].GetId() != strconv.Itoa(9-i) {
				t.Fatalf("%s failed: expected record %#v but received %#v", name, strconv.Itoa(9-i), boList[i].GetId())
			}
		}
	}
}

func TestCosmosdb_CreateGetManyWithFilterAndSorting(t *testing.T) {
	name := "TestCosmosdb_CreateGetManyWithFilterAndSorting"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
		ubo.SetExtraAttr(colCosmosdbPk, "users")
		ubo.SetDataAttr("name.first", strconv.Itoa(i))
		ubo.SetDataAttr("name.last", "Nguyen")
		ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
		ubo.SetExtraAttr("age", 35+i)
		if ok, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", name)
		}
	}

	filter := &godal.FilterOptFieldOpValue{FieldName: "email", Operator: godal.FilterOpLess, Value: "3@mydomain.com"}
	sorting := (&godal.SortingField{FieldName: "email", Descending: true}).ToSortingOpt()
	if boList, err := dao.GetAll(filter, sorting); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(boList) != 3 {
		t.Fatalf("%s failed: expected %#v items but received %#v", name, 3, len(boList))
	} else {
		if boList[0].GetId() != "2" || boList[1].GetId() != "1" || boList[2].GetId() != "0" {
			t.Fatalf("%s failed", name)
		}
	}
}

func TestCosmosdb_CreateGetManyWithSortingAndPaging(t *testing.T) {
	name := "TestCosmosdb_CreateGetManyWithSortingAndPaging"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
		ubo.SetExtraAttr(colCosmosdbPk, "users")
		ubo.SetDataAttr("name.first", strconv.Itoa(i))
		ubo.SetDataAttr("name.last", "Nguyen")
		ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
		ubo.SetExtraAttr("age", 35+i)
		if ok, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", name)
		}
	}

	fromOffset := 3
	numRows := 4
	sorting := (&godal.SortingField{FieldName: "email", Descending: true}).ToSortingOpt()
	if boList, err := dao.GetN(fromOffset, numRows, nil, sorting); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(boList) != numRows {
		t.Fatalf("%s failed: expected %#v items but received %#v", name, numRows, len(boList))
	} else {
		for i := 0; i < numRows; i++ {
			if boList[i].GetId() != strconv.Itoa(9-i-fromOffset) {
				t.Fatalf("%s failed: expected record %#v but received %#v", name, strconv.Itoa(9-i-fromOffset), boList[i].GetId())
			}
		}
	}
}

func TestCosmosdb_Update(t *testing.T) {
	name := "TestCosmosdb_Update"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")
	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(colCosmosdbPk, "users")
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if _, err := dao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	ubo.SetDataAttr("name.first", "Thanh2")
	ubo.SetDataAttr("name.last", "Nguyen2")
	ubo.SetExtraAttr("email", "thanh@mydomain.com")
	ubo.SetExtraAttr("age", 37)
	if ok, err := dao.Update(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot update record", name)
	}

	if bo, err := dao.Get("id"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", name)
	} else {
		if v := bo.GetTagVersion(); v != uint64(1357) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, uint64(1357), v)
		}
		if v := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh2" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh2", v)
		}
		if v := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen2" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen2", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "thanh@mydomain.com" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "thanh@mydomain.com", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, int64(37), v)
		}
	}
}

func TestCosmosdb_UpdateNotExist(t *testing.T) {
	name := "TestCosmosdb_UpdateNotExist"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")
	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(colCosmosdbPk, "users")
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := dao.Update(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if ok {
		t.Fatalf("%s failed: record should not be updated", name)
	}
}

func TestCosmosdb_UpdateDuplicated(t *testing.T) {
	name := "TestCosmosdb_UpdateDuplicated"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")

	ubo1 := NewUniversalBo("1", 1357)
	ubo1.SetExtraAttr(colCosmosdbPk, "users")
	ubo1.SetDataAttr("name.first", "Thanh")
	ubo1.SetDataAttr("name.last", "Nguyen")
	ubo1.SetExtraAttr("email", "1@mydomain.com")
	ubo1.SetExtraAttr("age", 35)
	if _, err := dao.Create(ubo1); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	ubo2 := NewUniversalBo("2", 1357)
	ubo2.SetExtraAttr(colCosmosdbPk, "users")
	ubo2.SetDataAttr("name.first", "Thanh2")
	ubo2.SetDataAttr("name.last", "Nguyen2")
	ubo2.SetExtraAttr("email", "2@mydomain.com")
	ubo2.SetExtraAttr("age", 35)
	if _, err := dao.Create(ubo2); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	ubo1.SetExtraAttr("email", "2@mydomain.com")
	if _, err := dao.Update(ubo1); err != godal.ErrGdaoDuplicatedEntry {
		t.Fatalf("%s failed: %s", name, err)
	}
}

func TestCosmosdb_SaveNew(t *testing.T) {
	name := "TestCosmosdb_SaveNew"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")
	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(colCosmosdbPk, "users")
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, old, err := dao.Save(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot save record", name)
	} else if old != nil {
		t.Fatalf("%s failed: there should be no existing record", name)
	}

	if bo, err := dao.Get("id"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", name)
	} else {
		if v := bo.GetTagVersion(); v != uint64(1357) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, uint64(1357), v)
		}
		if v := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh", v)
		}
		if v := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "myname@mydomain.com", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, int64(35), v)
		}
	}
}

func TestCosmosdb_SaveExisting(t *testing.T) {
	name := "TestCosmosdb_SaveExisting"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")
	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(colCosmosdbPk, "users")
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if _, err := dao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	ubo.SetDataAttr("name.first", "Thanh2")
	ubo.SetDataAttr("name.last", "Nguyen2")
	ubo.SetExtraAttr("email", "thanh@mydomain.com")
	ubo.SetExtraAttr("age", 37)
	if ok, old, err := dao.Save(ubo); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot save record", name)
	} else if old == nil {
		t.Fatalf("%s failed: there should be an existing record", name)
	} else {
		if v := old.GetTagVersion(); v != uint64(1357) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, uint64(1357), v)
		}
		if v := old.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh", v)
		}
		if v := old.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen", v)
		}
		if v := old.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "myname@mydomain.com", v)
		}
		if v := old.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, int64(35), v)
		}
	}

	if bo, err := dao.Get("id"); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", name)
	} else {
		if v := bo.GetTagVersion(); v != uint64(1357) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, uint64(1357), v)
		}
		if v := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh2" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh2", v)
		}
		if v := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen2" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen2", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "thanh@mydomain.com" {
			t.Fatalf("%s failed: expected %#v but received %#v", name, "thanh@mydomain.com", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, int64(37), v)
		}
	}
}

func TestCosmosdb_SaveExistingUnique(t *testing.T) {
	name := "TestCosmosdb_SaveExistingUnique"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")
	ubo1 := NewUniversalBo("1", 1357)
	ubo1.SetExtraAttr(colCosmosdbPk, "users")
	ubo1.SetDataAttr("name.first", "Thanh1")
	ubo1.SetDataAttr("name.last", "Nguyen1")
	ubo1.SetExtraAttr("email", "1@mydomain.com")
	ubo1.SetExtraAttr("age", 35)
	if _, err := dao.Create(ubo1); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	ubo2 := NewUniversalBo("2", 1357)
	ubo2.SetExtraAttr(colCosmosdbPk, "users")
	ubo2.SetDataAttr("name.first", "Thanh2")
	ubo2.SetDataAttr("name.last", "Nguyen2")
	ubo2.SetExtraAttr("email", "2@mydomain.com")
	ubo2.SetExtraAttr("age", 37)
	if _, err := dao.Create(ubo2); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	ubo1.SetExtraAttr("email", "2@mydomain.com")
	if _, _, err := dao.Save(ubo1); err != godal.ErrGdaoDuplicatedEntry {
		t.Fatalf("%s failed: %s", name, err)
	}
}

func TestCosmosdb_CreateUpdateGet_Checksum(t *testing.T) {
	name := "TestCosmosdb_CreateUpdateGet_Checksum"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, "users")

	_tagVersion := uint64(1337)
	_id := "admin@local"
	_maskId := "admin"
	_pwd := "mypassword"
	_displayName := "Administrator"
	_isAdmin := true
	_Email := "myname@mydomain.com"
	_Age := float64(35)
	user0 := newUser(_tagVersion, _id, _maskId)
	user0.SetExtraAttr(colCosmosdbPk, "users")
	user0.SetPassword(_pwd).SetDisplayName(_displayName).SetAdmin(_isAdmin)
	user0.SetDataAttr("name.first", "Thanh")
	user0.SetDataAttr("name.last", "Nguyen")
	user0.SetExtraAttr("email", _Email)
	user0.SetExtraAttr("age", _Age)
	if ok, err := dao.Create(&(user0.sync().UniversalBo)); err != nil {
		t.Fatalf("%s failed: %s", name+"/Create", err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", name)
	}
	if bo, err := dao.Get(_id); err != nil {
		t.Fatalf("%s failed: %s", name+"/Get", err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", name)
	} else {
		if v1, v0 := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString), "Thanh"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString), "Nguyen"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString), _Email; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt), int64(_Age); v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if bo.GetChecksum() != user0.GetChecksum() {
			csumMap := map[string]interface{}{
				"id":          bo.id,
				"app_version": bo.tagVersion,
				"t_created":   bo.timeCreated.In(time.UTC).Format(TimeLayout),
				"data":        bo._data,
				"extra":       bo._extraAttrs,
			}
			csum := fmt.Sprintf("%x", checksum.Md5Checksum(csumMap))
			fmt.Printf("DEBUG: %s - %s / %s\n", bo.GetChecksum(), csum, csumMap)

			csumMap = map[string]interface{}{
				"id":          user0.id,
				"app_version": user0.tagVersion,
				"t_created":   user0.timeCreated.In(time.UTC).Format(TimeLayout),
				"data":        user0._data,
				"extra":       user0._extraAttrs,
			}
			csum = fmt.Sprintf("%x", checksum.Md5Checksum(csumMap))
			fmt.Printf("DEBUG: %s - %s / %s\n", user0.GetChecksum(), csum, csumMap)

			t.Fatalf("%s failed: expected %#v but received %#v", name, user0.GetChecksum(), bo.GetChecksum())
		}

		user1 := newUserFromUbo(bo)
		if v1, v0 := user1.GetDataAttrAsUnsafe("name.first", reddo.TypeString), "Thanh"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetDataAttrAsUnsafe("name.last", reddo.TypeString), "Nguyen"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetExtraAttrAsUnsafe("email", reddo.TypeString), _Email; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetExtraAttrAsUnsafe("age", reddo.TypeInt), int64(_Age); v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetTagVersion(), _tagVersion; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetId(), _id; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetDisplayName(), _displayName; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetMaskId(), _maskId; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetPassword(), _pwd; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.IsAdmin(), _isAdmin; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if user1.GetChecksum() != user0.GetChecksum() {
			t.Fatalf("%s failed: expected %#v but received %#v", name, user0.GetChecksum(), user1.GetChecksum())
		}
	}

	oldChecksum := user0.GetChecksum()
	user0.SetMaskId(_maskId + "-new").SetPassword(_pwd + "-new").SetDisplayName(_displayName + "-new").SetAdmin(!_isAdmin).SetTagVersion(_tagVersion + 3)
	user0.SetDataAttr("name.first", "Thanh2")
	user0.SetDataAttr("name.last", "Nguyen2")
	user0.SetExtraAttr("email", _Email+"-new")
	user0.SetExtraAttr("age", _Age+2)
	if ok, err := dao.Update(&(user0.sync().UniversalBo)); err != nil {
		t.Fatalf("%s failed: %s", name+"/Update", err)
	} else if !ok {
		t.Fatalf("%s failed: cannot update record", name)
	}
	if bo, err := dao.Get(_id); err != nil {
		t.Fatalf("%s failed: %s", name+"/Get", err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", name)
	} else {
		if v1, v0 := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString), "Thanh2"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString), "Nguyen2"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString), _Email+"-new"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt), int64(_Age+2); v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if bo.GetChecksum() != user0.GetChecksum() {
			t.Fatalf("%s failed: expected %#v but received %#v", name, user0.GetChecksum(), bo.GetChecksum())
		}

		user1 := newUserFromUbo(bo)
		if v1, v0 := user1.GetDataAttrAsUnsafe("name.first", reddo.TypeString), "Thanh2"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetDataAttrAsUnsafe("name.last", reddo.TypeString), "Nguyen2"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetExtraAttrAsUnsafe("email", reddo.TypeString), _Email+"-new"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetExtraAttrAsUnsafe("age", reddo.TypeInt), int64(_Age+2); v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetTagVersion(), _tagVersion+3; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetId(), _id; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetDisplayName(), _displayName+"-new"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetMaskId(), _maskId+"-new"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.GetPassword(), _pwd+"-new"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if v1, v0 := user1.IsAdmin(), !_isAdmin; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", name, v0, v1)
		}
		if user1.GetChecksum() != user0.GetChecksum() {
			t.Fatalf("%s failed: expected %#v but received %#v", name, user0.GetChecksum(), user1.GetChecksum())
		}
		if user1.GetChecksum() == oldChecksum {
			t.Fatalf("%s failed: checksum must not be %#v", name, oldChecksum)
		}
	}
}
