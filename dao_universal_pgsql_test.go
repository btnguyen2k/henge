package henge

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/sql"
	"github.com/btnguyen2k/prom"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func _cleanupPgsql(sqlc *prom.SqlConnect, tableName string) error {
	_, err := sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	return err
}

func _testPgsqlInitSqlConnect(t *testing.T, testName, tableName string) *prom.SqlConnect {
	driver := strings.ReplaceAll(os.Getenv("PGSQL_DRIVER"), `"`, "")
	url := strings.ReplaceAll(os.Getenv("PGSQL_URL"), `"`, "")
	if driver == "" || url == "" {
		t.Skipf("%s skipped", testName)
		return nil
	}
	timezone := strings.ReplaceAll(os.Getenv("TIMEZONE"), `"`, "")
	if timezone == "" {
		timezone = "Asia/Ho_Chi_Minh"
	}
	urlTimezone := strings.ReplaceAll(timezone, "/", "%2f")
	url = strings.ReplaceAll(url, "${loc}", urlTimezone)
	url = strings.ReplaceAll(url, "${tz}", urlTimezone)
	url = strings.ReplaceAll(url, "${timezone}", urlTimezone)

	sqlc, err := NewPgsqlConnection(url, timezone, driver, 10000, nil)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewPgsqlConnection", err)
	}
	if err := _cleanupPgsql(sqlc, tableName); err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "_cleanupPgsql", err)
	}
	return sqlc
}

func TestNewPgsqlConnection(t *testing.T) {
	name := "TestNewPgsqlConnection"
	sqlc := _testPgsqlInitSqlConnect(t, name, "table_temp")
	defer sqlc.Close()
}

func TestInitPgsqlTable(t *testing.T) {
	name := "TestInitPgsqlTable"
	tblName := "table_temp"
	sqlc := _testPgsqlInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	for i := 0; i < 2; i++ {
		if err := InitPgsqlTable(sqlc, tblName, colDef); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}
}

func TestCreateIndexPgsql(t *testing.T) {
	name := "TestCreateIndexPgsql"
	tblName := "table_temp"
	sqlc := _testPgsqlInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitPgsqlTable(sqlc, tblName, colDef); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if err := CreateIndexSql(sqlc, tblName, true, []string{"col_email"}); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if err := CreateIndexSql(sqlc, tblName, false, []string{"col_age"}); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
}

func _testPgsqlInit(t *testing.T, name, tblName string) (*prom.SqlConnect, UniversalDao) {
	sqlc := _testPgsqlInitSqlConnect(t, name, tblName)
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitPgsqlTable(sqlc, tblName, colDef); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if err := CreateIndexSql(sqlc, tblName, true, []string{"col_email"}); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if err := CreateIndexSql(sqlc, tblName, false, []string{"col_age"}); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	extraColNameToFieldMappings := map[string]string{"col_email": "email", "col_age": "age"}
	return sqlc, NewUniversalDaoSql(sqlc, tblName, true, extraColNameToFieldMappings)
}

func TestPgsql_Create(t *testing.T) {
	name := "TestPgsql_Create"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()
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

func TestPgsql_CreateExistingPK(t *testing.T) {
	name := "TestPgsql_CreateExistingPK"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()
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
	if ok, err := dao.Create(ubo); err != godal.GdaoErrorDuplicatedEntry {
		t.Fatalf("%s failed: %s", name, err)
	} else if ok {
		t.Fatalf("%s failed: record should not be created twice", name)
	}
}

func TestPgsql_CreateExistingUnique(t *testing.T) {
	name := "TestPgsql_CreateExistingUnique"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()
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
	if ok, err := dao.Create(ubo); err != godal.GdaoErrorDuplicatedEntry {
		t.Fatalf("%s failed: %s", name, err)
	} else if ok {
		t.Fatalf("%s failed: record should not be created twice", name)
	}
}

func TestPgsql_CreateGet(t *testing.T) {
	name := "TestPgsql_CreateGet"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()
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

func TestPgsql_CreateDelete(t *testing.T) {
	name := "TestPgsql_CreateDelete"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()
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

func TestPgsql_CreateGetMany(t *testing.T) {
	name := "TestPgsql_CreateGetMany"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()

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

func TestPgsql_CreateGetManyWithFilter(t *testing.T) {
	name := "TestPgsql_CreateGetManyWithFilter"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()

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

	filter := &sql.FilterFieldValue{Field: "col_age", Operation: ">=", Value: 35 + 3}
	if boList, err := dao.GetAll(filter, nil); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(boList) != 7 {
		t.Fatalf("%s failed: expected %#v items but received %#v", name, 7, len(boList))
	}
}

func TestPgsql_CreateGetManyWithSorting(t *testing.T) {
	name := "TestPgsql_CreateGetManyWithSorting"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()

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

	sorting := map[string]string{"col_email": "desc"}
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

func TestPgsql_CreateGetManyWithFilterAndSorting(t *testing.T) {
	name := "TestPgsql_CreateGetManyWithFilterAndSorting"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()

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

	filter := &sql.FilterFieldValue{Field: "col_email", Operation: "<", Value: "3@mydomain.com"}
	sorting := map[string]string{"col_email": "desc"}
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

func TestPgsql_CreateGetManyWithSortingAndPaging(t *testing.T) {
	name := "TestPgsql_CreateGetManyWithSortingAndPaging"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()

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
	sorting := map[string]string{"col_email": "desc"}
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

func TestPgsql_Update(t *testing.T) {
	name := "TestPgsql_Update"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()
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

func TestPgsql_UpdateNotExist(t *testing.T) {
	name := "TestPgsql_UpdateNotExist"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()
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

func TestPgsql_UpdateDuplicated(t *testing.T) {
	name := "TestPgsql_UpdateDuplicated"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()

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
	if _, err := dao.Update(ubo1); err != godal.GdaoErrorDuplicatedEntry {
		t.Fatalf("%s failed: %s", name, err)
	}
}

func TestPgsql_SaveNew(t *testing.T) {
	name := "TestPgsql_SaveNew"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()
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

func TestPgsql_SaveExisting(t *testing.T) {
	name := "TestPgsql_SaveExisting"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()
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

func TestPgsql_SaveExistingUnique(t *testing.T) {
	name := "TestPgsql_SaveExistingUnique"
	tblName := "table_temp"
	sqlc, dao := _testPgsqlInit(t, name, tblName)
	defer sqlc.Close()
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
	if _, _, err := dao.Save(ubo1); err != godal.GdaoErrorDuplicatedEntry {
		t.Fatalf("%s failed: %s", name, err)
	}
}
