package henge

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	_ "github.com/btnguyen2k/gocosmos"
	"github.com/btnguyen2k/godal"
	prom "github.com/btnguyen2k/prom/sql"
)

func TestRowMapperCosmosdb_ToRow(t *testing.T) {
	testName := "TestRowMapperCosmosdb_ToRow"
	rm := buildRowMapperCosmosdb()
	gbo := godal.NewGenericBo()
	gbo.GboSetAttr(FieldId, "myid")
	gbo.GboSetAttr(FieldTagVersion, 123)
	now := time.Now().Round(time.Millisecond)
	gbo.GboSetAttr(FieldTimeCreated, now)
	next := now.Add(123 * time.Millisecond)
	gbo.GboSetAttr(FieldTimeUpdated, next)
	gbo.GboSetAttr(FieldData, `{"field":"value"}`)
	row, err := rm.ToRow("tbl_test", gbo)
	if err != nil || row == nil {
		t.Fatalf("%s failed: %s / %#v", testName, err, row)
	}

	expected := map[string]interface{}{
		FieldId:          "myid",
		FieldTagVersion:  123,
		FieldTimeCreated: now,
		FieldTimeUpdated: next,
		FieldData:        map[string]interface{}{"field": "value"},
	}
	if !reflect.DeepEqual(row, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", testName, expected, row)
	}
}

func TestRowMapperCosmosdb_ToBo(t *testing.T) {
	testName := "TestRowMapperCosmosdb_ToBo"
	rm := buildRowMapperCosmosdb()
	now := time.Now().Round(time.Millisecond)
	next := now.Add(123 * time.Millisecond)
	dataFieldValueList := []interface{}{`{"field":"value"}`, []byte(`{"field":"value"}`), map[string]string{"field": "value"}}
	for _, dataFieldValue := range dataFieldValueList {
		t.Run(fmt.Sprintf("%v", dataFieldValue), func(t *testing.T) {
			input := map[string]interface{}{
				FieldId:          "myid",
				FieldTagVersion:  123,
				FieldTimeCreated: now,
				FieldTimeUpdated: next,
				FieldData:        dataFieldValue,
			}
			bo, err := rm.ToBo("tbl_test", input)
			if bo == nil || err != nil {
				t.Fatalf("%s failed: %s / %#v", testName, err, bo)
			}

			if v, err := bo.GboGetAttr(FieldId, nil); err != nil || v != "myid" {
				t.Fatalf("%s failed: expected %#v but received %#v / %s", testName, "id", v, err)
			}
			if v, err := bo.GboGetAttr(FieldTagVersion, nil); err != nil || v != 123 {
				t.Fatalf("%s failed: expected %#v but received %#v / %s", testName, 123, v, err)
			}
			if v, err := bo.GboGetAttr(FieldTimeCreated, nil); err != nil || !now.Equal(v.(time.Time)) {
				t.Fatalf("%s failed: expected %#v but received %#v / %s", testName, now, v, err)
			}
			if v, err := bo.GboGetAttr(FieldTimeUpdated, nil); err != nil || !next.Equal(v.(time.Time)) {
				t.Fatalf("%s failed: expected %#v but received %#v / %s", testName, next, v, err)
			}
			if v, err := bo.GboGetAttr(FieldData, nil); err != nil || v != `{"field":"value"}` {
				t.Fatalf("%s failed: expected %#v but received %#v / %s", testName, `{"field":"value"}`, v, err)
			}
		})
	}
}

func TestCosmosdbFilterGeneratorSql(t *testing.T) {
	name := "TestCosmosdbFilterGeneratorSql"
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
	dbre := regexp.MustCompile(`(?i);db=(\w+)`)
	db := "godal"
	findResult := dbre.FindAllStringSubmatch(url, -1)
	if findResult == nil {
		url += ";Db=" + db
	} else {
		db = findResult[0][1]
	}
	sqlc, err := NewCosmosdbConnection(url, timezone, driver, 10000, nil)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewCosmosdbConnection", err)
	}
	sqlc.GetDB().Exec("CREATE DATABASE " + db + " WITH maxru=10000")
	if err := _cleanupCosmosdb(sqlc, tableName); err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "_cleanupCosmosdb", err)
	}
	return sqlc
}

const (
	testCosmosdbPkCol = "type"
	testCosmosdbPkVal = "users"
	testCosmosdbUk    = "/email"
)

var setupTestCosmosdb = func(t *testing.T, testName string) {
	testSqlc = _testCosmosdbInitSqlConnect(t, testName, testTable)
	collectionSpec := &CosmosdbCollectionSpec{Pk: testCosmosdbPkCol, Uk: [][]string{{testCosmosdbUk}}}
	if err := InitCosmosdbCollection(testSqlc, testTable, collectionSpec); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	daoSpec := &CosmosdbDaoSpec{PkName: testCosmosdbPkCol, PkValue: testCosmosdbPkVal, TxModeOnWrite: true}
	testDao = NewUniversalDaoCosmosdbSql(testSqlc, testTable, daoSpec)
}

var teardownTestCosmosdb = func(t *testing.T, testName string) {
	if testSqlc != nil {
		testSqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))
		testSqlc.Close()
		defer func() { testSqlc = nil }()
	}
}

func TestInitCosmosdbCollection(t *testing.T) {
	testName := "TestInitCosmosdbCollection"

	specList := []*CosmosdbCollectionSpec{
		{Pk: "pk", Ru: 400},
		{LargePk: "largepk", MaxRu: 4000},
		{Pk: "pk", Uk: [][]string{{"/uk1"}, {"/uk2a", "/uk2b"}}},
	}
	for _, spec := range specList {
		t.Run(fmt.Sprintf("%v", *spec), func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			if err := InitCosmosdbCollection(testSqlc, testTable, spec); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}
		})
	}
}

// func _testCosmosdbInit(t *testing.T, name string, sqlc *prom.SqlConnect, tblName, pkName, pkValue string) UniversalDao {
// 	collectionSpec := &CosmosdbCollectionSpec{Pk: pkName, Uk: [][]string{{"/email"}}}
// 	if err := InitCosmosdbCollection(sqlc, tblName, collectionSpec); err != nil {
// 		t.Fatalf("%s failed: %s", name, err)
// 	}
// 	daoSpec := &CosmosdbDaoSpec{PkName: pkName, PkValue: pkValue, TxModeOnWrite: true}
// 	dao := NewUniversalDaoCosmosdbSql(sqlc, tblName, daoSpec)
// 	return dao
// }

func TestNewUniversalDaoCosmosdbSql_nil(t *testing.T) {
	name := "TestNewUniversalDaoCosmosdbSql_nil"
	dao := NewUniversalDaoCosmosdbSql(nil, "", nil)
	if dao != nil {
		t.Fatalf("%s failed: expected nil", name)
	}
}

func TestNewUniversalDaoCosmosdbSql(t *testing.T) {
	testName := "TestNewUniversalDaoCosmosdbSql"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	if cdao, _ := testDao.(*UniversalDaoCosmosdbSql); cdao == nil {
		t.Fatalf("%s failed: not *UniversalDaoCosmosdbSql", testName)
	} else if v := cdao.GetPkName(); v != testCosmosdbPkCol {
		t.Fatalf("%s failed: expected %#v but received %#v", testName, testCosmosdbPkCol, v)
	} else if v := cdao.GetPkValue(); v != testCosmosdbPkVal {
		t.Fatalf("%s failed: expected %#v but received %#v", testName, testCosmosdbPkVal, v)
	}
}

func TestUniversalDaoCosmosdbSql_Create(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_Create"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := testDao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", testName)
	}
}

func TestUniversalDaoCosmosdbSql_CreateExistingPK(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_CreateExistingPK"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := testDao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", testName)
	}

	ubo.SetExtraAttr("email", "myname2@mydomain.com")
	if ok, err := testDao.Create(ubo); err != godal.ErrGdaoDuplicatedEntry {
		t.Fatalf("%s failed: %s", testName, err)
	} else if ok {
		t.Fatalf("%s failed: record should not be created twice", testName)
	}
}

func TestUniversalDaoCosmosdbSql_CreateExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_CreateExistingUnique"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := testDao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", testName)
	}

	ubo.SetId("id2")
	if ok, err := testDao.Create(ubo); err != godal.ErrGdaoDuplicatedEntry {
		t.Fatalf("%s failed: %s", testName, err)
	} else if ok {
		t.Fatalf("%s failed: record should not be created twice", testName)
	}
}

func TestUniversalDaoCosmosdbSql_CreateGet(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_CreateGet"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := testDao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", testName)
	}

	if bo, err := testDao.Get("id"); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", testName)
	} else {
		if v := bo.GetTagVersion(); v != uint64(1357) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, uint64(1357), v)
		}
		if v := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh", v)
		}
		if v := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "myname@mydomain.com", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(35), v)
		}
	}
}

func TestUniversalDaoCosmosdbSql_CreateDelete(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_CreateDelete"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := testDao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", testName)
	}

	if ok, err := testDao.Delete(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot delete record", testName)
	}

	if bo, err := testDao.Get("id"); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if bo != nil {
		t.Fatalf("%s failed: record should be deleted", testName)
	}

	if ok, err := testDao.Delete(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if ok {
		t.Fatalf("%s failed: record should not be deleted twice", testName)
	}
}

func TestUniversalDaoCosmosdbSql_CreateGetMany(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_CreateGetMany"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
		ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
		ubo.SetDataAttr("testName.first", strconv.Itoa(i))
		ubo.SetDataAttr("testName.last", "Nguyen")
		ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
		ubo.SetExtraAttr("age", 35+i)
		if ok, err := testDao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", testName)
		}
	}

	if boList, err := testDao.GetAll(nil, nil); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(boList) != 10 {
		t.Fatalf("%s failed: expected %#v items but received %#v", testName, 10, len(boList))
	}
}

func TestUniversalDaoCosmosdbSql_CreateGetManyWithFilter(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_CreateGetManyWithFilter"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
		ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
		ubo.SetDataAttr("testName.first", strconv.Itoa(i))
		ubo.SetDataAttr("testName.last", "Nguyen")
		ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
		ubo.SetExtraAttr("age", 35+i)
		if ok, err := testDao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", testName)
		}
	}

	filter := &godal.FilterOptFieldOpValue{FieldName: "age", Operator: godal.FilterOpGreaterOrEqual, Value: 35 + 3}
	if boList, err := testDao.GetAll(filter, nil); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(boList) != 7 {
		t.Fatalf("%s failed: expected %#v items but received %#v", testName, 7, len(boList))
	}
}

func TestUniversalDaoCosmosdbSql_CreateGetManyWithSorting(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_CreateGetManyWithSorting"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
		ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
		ubo.SetDataAttr("testName.first", strconv.Itoa(i))
		ubo.SetDataAttr("testName.last", "Nguyen")
		ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
		ubo.SetExtraAttr("age", 35+i)
		if ok, err := testDao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", testName)
		}
	}

	sorting := (&godal.SortingField{FieldName: "email", Descending: true}).ToSortingOpt()
	if boList, err := testDao.GetAll(nil, sorting); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else {
		for i := 0; i < 10; i++ {
			if boList[i].GetId() != strconv.Itoa(9-i) {
				t.Fatalf("%s failed: expected record %#v but received %#v", testName, strconv.Itoa(9-i), boList[i].GetId())
			}
		}
	}
}

func TestUniversalDaoCosmosdbSql_CreateGetManyWithFilterAndSorting(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_CreateGetManyWithFilterAndSorting"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
		ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
		ubo.SetDataAttr("testName.first", strconv.Itoa(i))
		ubo.SetDataAttr("testName.last", "Nguyen")
		ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
		ubo.SetExtraAttr("age", 35+i)
		if ok, err := testDao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", testName)
		}
	}

	filter := &godal.FilterOptFieldOpValue{FieldName: "email", Operator: godal.FilterOpLess, Value: "3@mydomain.com"}
	sorting := (&godal.SortingField{FieldName: "email", Descending: true}).ToSortingOpt()
	if boList, err := testDao.GetAll(filter, sorting); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(boList) != 3 {
		t.Fatalf("%s failed: expected %#v items but received %#v", testName, 3, len(boList))
	} else {
		if boList[0].GetId() != "2" || boList[1].GetId() != "1" || boList[2].GetId() != "0" {
			t.Fatalf("%s failed", testName)
		}
	}
}

func TestUniversalDaoCosmosdbSql_CreateGetManyWithSortingAndPaging(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_CreateGetManyWithSortingAndPaging"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
		ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
		ubo.SetDataAttr("testName.first", strconv.Itoa(i))
		ubo.SetDataAttr("testName.last", "Nguyen")
		ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
		ubo.SetExtraAttr("age", 35+i)
		if ok, err := testDao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", testName)
		}
	}

	fromOffset := 3
	numRows := 4
	sorting := (&godal.SortingField{FieldName: "email", Descending: true}).ToSortingOpt()
	if boList, err := testDao.GetN(fromOffset, numRows, nil, sorting); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(boList) != numRows {
		t.Fatalf("%s failed: expected %#v items but received %#v", testName, numRows, len(boList))
	} else {
		for i := 0; i < numRows; i++ {
			if boList[i].GetId() != strconv.Itoa(9-i-fromOffset) {
				t.Fatalf("%s failed: expected record %#v but received %#v", testName, strconv.Itoa(9-i-fromOffset), boList[i].GetId())
			}
		}
	}
}

func TestUniversalDaoCosmosdbSql_Update(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_Update"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if _, err := testDao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	ubo.SetDataAttr("testName.first", "Thanh2")
	ubo.SetDataAttr("testName.last", "Nguyen2")
	ubo.SetExtraAttr("email", "thanh@mydomain.com")
	ubo.SetExtraAttr("age", 37)
	if ok, err := testDao.Update(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot update record", testName)
	}

	if bo, err := testDao.Get("id"); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", testName)
	} else {
		if v := bo.GetTagVersion(); v != uint64(1357) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, uint64(1357), v)
		}
		if v := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh2" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh2", v)
		}
		if v := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen2" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen2", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "thanh@mydomain.com" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "thanh@mydomain.com", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(37), v)
		}
	}
}

func TestUniversalDaoCosmosdbSql_UpdateNotExist(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_UpdateNotExist"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, err := testDao.Update(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if ok {
		t.Fatalf("%s failed: record should not be updated", testName)
	}
}

func TestUniversalDaoCosmosdbSql_UpdateDuplicated(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_UpdateDuplicated"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	ubo1 := NewUniversalBo("1", 1357)
	ubo1.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo1.SetDataAttr("testName.first", "Thanh")
	ubo1.SetDataAttr("testName.last", "Nguyen")
	ubo1.SetExtraAttr("email", "1@mydomain.com")
	ubo1.SetExtraAttr("age", 35)
	if _, err := testDao.Create(ubo1); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	ubo2 := NewUniversalBo("2", 1357)
	ubo2.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo2.SetDataAttr("testName.first", "Thanh2")
	ubo2.SetDataAttr("testName.last", "Nguyen2")
	ubo2.SetExtraAttr("email", "2@mydomain.com")
	ubo2.SetExtraAttr("age", 35)
	if _, err := testDao.Create(ubo2); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	ubo1.SetExtraAttr("email", "2@mydomain.com")
	if _, err := testDao.Update(ubo1); err != godal.ErrGdaoDuplicatedEntry {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

func TestUniversalDaoCosmosdbSql_SaveNew(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SaveNew"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if ok, old, err := testDao.Save(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot save record", testName)
	} else if old != nil {
		t.Fatalf("%s failed: there should be no existing record", testName)
	}

	if bo, err := testDao.Get("id"); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", testName)
	} else {
		if v := bo.GetTagVersion(); v != uint64(1357) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, uint64(1357), v)
		}
		if v := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh", v)
		}
		if v := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "myname@mydomain.com", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(35), v)
		}
	}
}

func TestUniversalDaoCosmosdbSql_SaveExisting(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SaveExisting"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
	ubo.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("age", 35)

	if _, err := testDao.Create(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	ubo.SetDataAttr("testName.first", "Thanh2")
	ubo.SetDataAttr("testName.last", "Nguyen2")
	ubo.SetExtraAttr("email", "thanh@mydomain.com")
	ubo.SetExtraAttr("age", 37)
	if ok, old, err := testDao.Save(ubo); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if !ok {
		t.Fatalf("%s failed: cannot save record", testName)
	} else if old == nil {
		t.Fatalf("%s failed: there should be an existing record", testName)
	} else {
		if v := old.GetTagVersion(); v != uint64(1357) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, uint64(1357), v)
		}
		if v := old.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh", v)
		}
		if v := old.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen", v)
		}
		if v := old.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "myname@mydomain.com", v)
		}
		if v := old.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(35), v)
		}
	}

	if bo, err := testDao.Get("id"); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", testName)
	} else {
		if v := bo.GetTagVersion(); v != uint64(1357) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, uint64(1357), v)
		}
		if v := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh2" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh2", v)
		}
		if v := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen2" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen2", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "thanh@mydomain.com" {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, "thanh@mydomain.com", v)
		}
		if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(37), v)
		}
	}
}

func TestUniversalDaoCosmosdbSql_SaveExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SaveExistingUnique"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	ubo1 := NewUniversalBo("1", 1357)
	ubo1.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo1.SetDataAttr("testName.first", "Thanh1")
	ubo1.SetDataAttr("testName.last", "Nguyen1")
	ubo1.SetExtraAttr("email", "1@mydomain.com")
	ubo1.SetExtraAttr("age", 35)
	if _, err := testDao.Create(ubo1); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	ubo2 := NewUniversalBo("2", 1357)
	ubo2.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	ubo2.SetDataAttr("testName.first", "Thanh2")
	ubo2.SetDataAttr("testName.last", "Nguyen2")
	ubo2.SetExtraAttr("email", "2@mydomain.com")
	ubo2.SetExtraAttr("age", 37)
	if _, err := testDao.Create(ubo2); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	ubo1.SetExtraAttr("email", "2@mydomain.com")
	if _, _, err := testDao.Save(ubo1); err != godal.ErrGdaoDuplicatedEntry {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

func TestUniversalDaoCosmosdbSql_CreateUpdateGet_Checksum(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_CreateUpdateGet_Checksum"
	teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
	defer teardownTest(t)

	_tagVersion := uint64(1337)
	_id := "admin@local"
	_maskId := "admin"
	_pwd := "mypassword"
	_displayName := "Administrator"
	_isAdmin := true
	_Email := "myname@mydomain.com"
	_Age := float64(35)
	user0 := newUser(_tagVersion, _id, _maskId)
	user0.SetExtraAttr(testCosmosdbPkCol, testCosmosdbPkVal)
	user0.SetPassword(_pwd).SetDisplayName(_displayName).SetAdmin(_isAdmin)
	user0.SetDataAttr("testName.first", "Thanh")
	user0.SetDataAttr("testName.last", "Nguyen")
	user0.SetExtraAttr("email", _Email)
	user0.SetExtraAttr("age", _Age)
	if ok, err := testDao.Create(&(user0.sync().UniversalBo)); err != nil {
		t.Fatalf("%s failed: %s", testName+"/Create", err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", testName)
	}
	if bo, err := testDao.Get(_id); err != nil {
		t.Fatalf("%s failed: %s", testName+"/Get", err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", testName)
	} else {
		if v1, v0 := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString), "Thanh"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString), "Nguyen"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString), _Email; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt), int64(_Age); v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if bo.GetChecksum() != user0.GetChecksum() {
			fmt.Printf("%s vs %s\n", bo.timeCreated, user0.timeCreated)
			t.Fatalf("%s failed: expected %#v but received %#v", testName, user0.GetChecksum(), bo.GetChecksum())
		}

		user1 := newUserFromUbo(bo)
		if v1, v0 := user1.GetDataAttrAsUnsafe("testName.first", reddo.TypeString), "Thanh"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetDataAttrAsUnsafe("testName.last", reddo.TypeString), "Nguyen"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetExtraAttrAsUnsafe("email", reddo.TypeString), _Email; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetExtraAttrAsUnsafe("age", reddo.TypeInt), int64(_Age); v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetTagVersion(), _tagVersion; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetId(), _id; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetDisplayName(), _displayName; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetMaskId(), _maskId; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetPassword(), _pwd; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.IsAdmin(), _isAdmin; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if user1.GetChecksum() != user0.GetChecksum() {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, user0.GetChecksum(), user1.GetChecksum())
		}
	}

	oldChecksum := user0.GetChecksum()
	user0.SetMaskId(_maskId + "-new").SetPassword(_pwd + "-new").SetDisplayName(_displayName + "-new").SetAdmin(!_isAdmin).SetTagVersion(_tagVersion + 3)
	user0.SetDataAttr("testName.first", "Thanh2")
	user0.SetDataAttr("testName.last", "Nguyen2")
	user0.SetExtraAttr("email", _Email+"-new")
	user0.SetExtraAttr("age", _Age+2)
	if ok, err := testDao.Update(&(user0.sync().UniversalBo)); err != nil {
		t.Fatalf("%s failed: %s", testName+"/Update", err)
	} else if !ok {
		t.Fatalf("%s failed: cannot update record", testName)
	}
	if bo, err := testDao.Get(_id); err != nil {
		t.Fatalf("%s failed: %s", testName+"/Get", err)
	} else if bo == nil {
		t.Fatalf("%s failed: not found", testName)
	} else {
		if v1, v0 := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString), "Thanh2"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString), "Nguyen2"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString), _Email+"-new"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt), int64(_Age+2); v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if bo.GetChecksum() != user0.GetChecksum() {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, user0.GetChecksum(), bo.GetChecksum())
		}

		user1 := newUserFromUbo(bo)
		if v1, v0 := user1.GetDataAttrAsUnsafe("testName.first", reddo.TypeString), "Thanh2"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetDataAttrAsUnsafe("testName.last", reddo.TypeString), "Nguyen2"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetExtraAttrAsUnsafe("email", reddo.TypeString), _Email+"-new"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetExtraAttrAsUnsafe("age", reddo.TypeInt), int64(_Age+2); v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetTagVersion(), _tagVersion+3; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetId(), _id; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetDisplayName(), _displayName+"-new"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetMaskId(), _maskId+"-new"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.GetPassword(), _pwd+"-new"; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if v1, v0 := user1.IsAdmin(), !_isAdmin; v1 != v0 {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, v0, v1)
		}
		if user1.GetChecksum() != user0.GetChecksum() {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, user0.GetChecksum(), user1.GetChecksum())
		}
		if user1.GetChecksum() == oldChecksum {
			t.Fatalf("%s failed: checksum must not be %#v", testName, oldChecksum)
		}
	}
}
