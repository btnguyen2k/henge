package henge

import (
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
)

func TestRowMapperMongo_ToRow(t *testing.T) {
	name := "TestRowMapperMongo_ToRow"
	rm := buildRowMapperMongo()
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

func TestRowMapperMongo_ToBo(t *testing.T) {
	name := "TestRowMapperMongo_ToBo"
	rm := buildRowMapperMongo()
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

func TestRowMapperMongo_ColumnsList(t *testing.T) {
	name := "TestRowMapperMongo_ColumnsList"
	rm := buildRowMapperMongo()
	if colList := rm.ColumnsList("*"); len(colList) == 0 {
		t.Fatalf("%s failed: 0", name)
	}
}

func _testMongoInitMongoConnect(t *testing.T, testName, collectionName string) *prom.MongoConnect {
	mongoUrl := strings.ReplaceAll(os.Getenv("MONGO_URL"), `"`, "")
	if mongoUrl == "" {
		t.Skipf("%s skipped", testName)
		return nil
	}
	mongoDb := strings.ReplaceAll(os.Getenv("MONGO_DB"), `"`, "")
	if mongoDb == "" {
		mongoDb = "test"
	}
	mc, err := prom.NewMongoConnectWithPoolOptions(mongoUrl, mongoDb, 10000, &prom.MongoPoolOpts{
		ConnectTimeout:         10 * time.Second,
		SocketTimeout:          10 * time.Second,
		ServerSelectionTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewMongoConnect", err)
	}
	return mc
}

func TestNewMongoConnection(t *testing.T) {
	name := "TestNewMongoConnection"
	mc := _testMongoInitMongoConnect(t, name, "table_temp")
	defer mc.Close(nil)
}

func TestInitMongoCollection(t *testing.T) {
	name := "TestInitMongoCollection"
	collectionName := "table_temp"
	mc := _testMongoInitMongoConnect(t, name, collectionName)
	defer mc.Close(nil)
	for i := 0; i < 2; i++ {
		if err := InitMongoCollection(mc, collectionName); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}
}

var setupTestMongo = func(t *testing.T, testName string) {
	testMc = _testMongoInitMongoConnect(t, testName, testTable)
	if err := InitMongoCollection(testMc, testTable); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	testMc.GetCollection(testTable).Drop(nil)
	index := map[string]interface{}{
		"key":    map[string]interface{}{"email": 1},
		"name":   "uidx_email",
		"unique": true,
	}
	testMc.CreateCollectionIndexes(testTable, []interface{}{index})
	mongoUrl := strings.ReplaceAll(os.Getenv("MONGO_URL"), `"`, "")
	txModeOnWrite := strings.Contains(mongoUrl, "replicaSet=")
	testDao = NewUniversalDaoMongo(testMc, testTable, txModeOnWrite)
}

var teardownTestMongo = func(t *testing.T, testName string) {
	if testMc != nil {
		defer func() { testMc = nil }()
		testMc.GetCollection(testTable).Drop(nil)
		testMc.Close(nil)
	}
}

func TestUniversalDaoMongo_Create(t *testing.T) {
	testName := "TestUniversalDaoMongo_Create"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
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

func TestUniversalDaoMongo_CreateExistingPK(t *testing.T) {
	testName := "TestUniversalDaoMongo_CreateExistingPK"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
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

func TestUniversalDaoMongo_CreateExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoMongo_CreateExistingUnique"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
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

func TestUniversalDaoMongo_CreateGet(t *testing.T) {
	testName := "TestUniversalDaoMongo_CreateGet"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
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

func TestUniversalDaoMongo_CreateDelete(t *testing.T) {
	testName := "TestUniversalDaoMongo_CreateDelete"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
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

func TestUniversalDaoMongo_CreateGetMany(t *testing.T) {
	testName := "TestUniversalDaoMongo_CreateGetMany"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
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

func TestUniversalDaoMongo_CreateGetManyWithFilter(t *testing.T) {
	testName := "TestUniversalDaoMongo_CreateGetManyWithFilter"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
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

func TestUniversalDaoMongo_CreateGetManyWithSorting(t *testing.T) {
	testName := "TestUniversalDaoMongo_CreateGetManyWithSorting"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
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

func TestUniversalDaoMongo_CreateGetManyWithFilterAndSorting(t *testing.T) {
	testName := "TestUniversalDaoMongo_CreateGetManyWithFilterAndSorting"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
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

	filter := godal.FilterOptFieldOpValue{FieldName: "email", Operator: godal.FilterOpLess, Value: "3@mydomain.com"}
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

func TestUniversalDaoMongo_CreateGetManyWithSortingAndPaging(t *testing.T) {
	testName := "TestUniversalDaoMongo_CreateGetManyWithSortingAndPaging"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
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

func TestUniversalDaoMongo_Update(t *testing.T) {
	testName := "TestUniversalDaoMongo_Update"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
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

func TestUniversalDaoMongo_UpdateNotExist(t *testing.T) {
	testName := "TestUniversalDaoMongo_UpdateNotExist"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
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

func TestUniversalDaoMongo_UpdateDuplicated(t *testing.T) {
	testName := "TestUniversalDaoMongo_UpdateDuplicated"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	ubo1 := NewUniversalBo("1", 1357)
	ubo1.SetDataAttr("testName.first", "Thanh")
	ubo1.SetDataAttr("testName.last", "Nguyen")
	ubo1.SetExtraAttr("email", "1@mydomain.com")
	ubo1.SetExtraAttr("age", 35)
	if _, err := testDao.Create(ubo1); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	ubo2 := NewUniversalBo("2", 1357)
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

func TestUniversalDaoMongo_SaveNew(t *testing.T) {
	testName := "TestUniversalDaoMongo_SaveNew"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
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

func TestUniversalDaoMongo_SaveExisting(t *testing.T) {
	testName := "TestUniversalDaoMongo_SaveExisting"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	ubo := NewUniversalBo("id", 1357)
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

func TestUniversalDaoMongo_SaveExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoMongo_SaveExistingUnique"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
	defer teardownTest(t)

	ubo1 := NewUniversalBo("1", 1357)
	ubo1.SetDataAttr("testName.first", "Thanh1")
	ubo1.SetDataAttr("testName.last", "Nguyen1")
	ubo1.SetExtraAttr("email", "1@mydomain.com")
	ubo1.SetExtraAttr("age", 35)
	if _, err := testDao.Create(ubo1); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	ubo2 := NewUniversalBo("2", 1357)
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

func TestUniversalDaoMongo_CreateUpdateGet_Checksum(t *testing.T) {
	testName := "TestUniversalDaoMongo_CreateUpdateGet_Checksum"
	teardownTest := setupTest(t, testName, setupTestMongo, teardownTestMongo)
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
