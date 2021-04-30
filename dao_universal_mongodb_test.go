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

func _cleanupMongo(mc *prom.MongoConnect, collectionName string) error {
	return mc.GetCollection(collectionName).Drop(nil)
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
	if err := _cleanupMongo(mc, collectionName); err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "_cleanupMongo", err)
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

func _testMongoInit(t *testing.T, name, collectionName string) (*prom.MongoConnect, UniversalDao) {
	mc := _testMongoInitMongoConnect(t, name, collectionName)
	if err := InitMongoCollection(mc, collectionName); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	index := map[string]interface{}{
		"key":    map[string]interface{}{"email": 1},
		"name":   "uidx_email",
		"unique": true,
	}
	mc.CreateCollectionIndexes(collectionName, []interface{}{index})
	mongoUrl := strings.ReplaceAll(os.Getenv("MONGO_URL"), `"`, "")
	txModeOnWrite := strings.Contains(mongoUrl, "replicaSet=")
	if txModeOnWrite {
		fmt.Println("txModeOnWrite:", txModeOnWrite)
	}
	return mc, NewUniversalDaoMongo(mc, collectionName, txModeOnWrite)
}

func TestMongo_Create(t *testing.T) {
	name := "TestMongo_Create"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)
	ubo := NewUniversalBo("id", 1357)
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

func TestMongo_CreateExistingPK(t *testing.T) {
	name := "TestMongo_CreateExistingPK"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)
	ubo := NewUniversalBo("id", 1357)
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

func TestMongo_CreateExistingUnique(t *testing.T) {
	name := "TestMongo_CreateExistingUnique"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)
	ubo := NewUniversalBo("id", 1357)
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

func TestMongo_CreateGet(t *testing.T) {
	name := "TestMongo_CreateGet"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)
	ubo := NewUniversalBo("id", 1357)
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

func TestMongo_CreateDelete(t *testing.T) {
	name := "TestMongo_CreateDelete"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)
	ubo := NewUniversalBo("id", 1357)
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

func TestMongo_CreateGetMany(t *testing.T) {
	name := "TestMongo_CreateGetMany"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
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

func TestMongo_CreateGetManyWithFilter(t *testing.T) {
	name := "TestMongo_CreateGetManyWithFilter"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
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

func TestMongo_CreateGetManyWithSorting(t *testing.T) {
	name := "TestMongo_CreateGetManyWithSorting"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
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

func TestMongo_CreateGetManyWithFilterAndSorting(t *testing.T) {
	name := "TestMongo_CreateGetManyWithFilterAndSorting"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
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

	filter := godal.FilterOptFieldOpValue{FieldName: "email", Operator: godal.FilterOpLess, Value: "3@mydomain.com"}
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

func TestMongo_CreateGetManyWithSortingAndPaging(t *testing.T) {
	name := "TestMongo_CreateGetManyWithSortingAndPaging"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for i := 0; i < 10; i++ {
		ubo := NewUniversalBo(idList[i], uint64(i))
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

func TestMongo_Update(t *testing.T) {
	name := "TestMongo_Update"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)
	ubo := NewUniversalBo("id", 1357)
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

func TestMongo_UpdateNotExist(t *testing.T) {
	name := "TestMongo_UpdateNotExist"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)
	ubo := NewUniversalBo("id", 1357)
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

func TestMongo_UpdateDuplicated(t *testing.T) {
	name := "TestMongo_UpdateDuplicated"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)

	ubo1 := NewUniversalBo("1", 1357)
	ubo1.SetDataAttr("name.first", "Thanh")
	ubo1.SetDataAttr("name.last", "Nguyen")
	ubo1.SetExtraAttr("email", "1@mydomain.com")
	ubo1.SetExtraAttr("age", 35)
	if _, err := dao.Create(ubo1); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	ubo2 := NewUniversalBo("2", 1357)
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

func TestMongo_SaveNew(t *testing.T) {
	name := "TestMongo_SaveNew"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)
	ubo := NewUniversalBo("id", 1357)
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

func TestMongo_SaveExisting(t *testing.T) {
	name := "TestMongo_SaveExisting"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)
	ubo := NewUniversalBo("id", 1357)
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

func TestMongo_SaveExistingUnique(t *testing.T) {
	name := "TestMongo_SaveExistingUnique"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)
	ubo1 := NewUniversalBo("1", 1357)
	ubo1.SetDataAttr("name.first", "Thanh1")
	ubo1.SetDataAttr("name.last", "Nguyen1")
	ubo1.SetExtraAttr("email", "1@mydomain.com")
	ubo1.SetExtraAttr("age", 35)
	if _, err := dao.Create(ubo1); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	ubo2 := NewUniversalBo("2", 1357)
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

func TestMongo_CreateUpdateGet_Checksum(t *testing.T) {
	name := "TestMongo_CreateUpdateGet_Checksum"
	collectionName := "table_temp"
	mc, dao := _testMongoInit(t, name, collectionName)
	defer mc.Close(nil)

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
			// csumMap := map[string]interface{}{
			// 	"id":          user1.id,
			// 	"app_version": user1.tagVersion,
			// 	"t_created":   user1.timeCreated.In(time.UTC).Format(TimeLayout),
			// 	"data":        user1._data,
			// 	"extra":       user1._extraAttrs,
			// }
			// csum := fmt.Sprintf("%x", checksum.Md5Checksum(csumMap))
			// fmt.Printf("DEBUG: %s - %s / %s\n", user1.GetChecksum(), csum, csumMap)
			//
			// csumMap = map[string]interface{}{
			// 	"id":          user0.id,
			// 	"app_version": user0.tagVersion,
			// 	"t_created":   user0.timeCreated.In(time.UTC).Format(TimeLayout),
			// 	"data":        user0._data,
			// 	"extra":       user0._extraAttrs,
			// }
			// csum = fmt.Sprintf("%x", checksum.Md5Checksum(csumMap))
			// fmt.Printf("DEBUG: %s - %s / %s\n", user0.GetChecksum(), csum, csumMap)

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
