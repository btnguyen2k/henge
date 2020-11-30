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
	_ "github.com/godror/godror"
)

func _cleanupOracle(sqlc *prom.SqlConnect, tableName string) error {
	_, err := sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	return err
}

func _testOracleInitSqlConnect(t *testing.T, testName, tableName string) *prom.SqlConnect {
	driver := strings.ReplaceAll(os.Getenv("ORACLE_DRIVER"), `"`, "")
	url := strings.ReplaceAll(os.Getenv("ORACLE_URL"), `"`, "")
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

	sqlc, err := NewOracleConnection(url, timezone, driver, 10000, nil)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewOracleConnection", err)
	}
	if err := _cleanupOracle(sqlc, tableName); err != nil && strings.Index(fmt.Sprintf("%s", err), "ORA-00942") < 0 {
		t.Fatalf("%s/%s failed: %s", testName, "_cleanupOracle", err)
	}
	return sqlc
}

func TestNewOracleConnection(t *testing.T) {
	name := "TestNewOracleConnection"
	sqlc := _testOracleInitSqlConnect(t, name, "table_temp")
	defer sqlc.Close()
}

func TestInitOracleTable(t *testing.T) {
	name := "TestInitOracleTable"
	tblName := "table_temp"
	sqlc := _testOracleInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitOracleTable(sqlc, tblName, colDef); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
}

func TestCreateIndexOracle(t *testing.T) {
	name := "TestCreateIndexOracle"
	tblName := "table_temp"
	sqlc := _testOracleInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitOracleTable(sqlc, tblName, colDef); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if err := CreateIndexSql(sqlc, tblName, true, []string{"col_email"}); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if err := CreateIndexSql(sqlc, tblName, false, []string{"col_age"}); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
}

func _testOracleInit(t *testing.T, name, tblName string) (*prom.SqlConnect, UniversalDao) {
	sqlc := _testOracleInitSqlConnect(t, name, tblName)
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitOracleTable(sqlc, tblName, colDef); err != nil {
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

func TestOracle_Create(t *testing.T) {
	name := "TestOracle_Create"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_CreateExistingPK(t *testing.T) {
	name := "TestOracle_CreateExistingPK"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_CreateExistingUnique(t *testing.T) {
	name := "TestOracle_CreateExistingUnique"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_CreateGet(t *testing.T) {
	name := "TestOracle_CreateGet"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_CreateDelete(t *testing.T) {
	name := "TestOracle_CreateDelete"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_CreateGetMany(t *testing.T) {
	name := "TestOracle_CreateGetMany"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_CreateGetManyWithFilter(t *testing.T) {
	name := "TestOracle_CreateGetManyWithFilter"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_CreateGetManyWithSorting(t *testing.T) {
	name := "TestOracle_CreateGetManyWithSorting"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_CreateGetManyWithFilterAndSorting(t *testing.T) {
	name := "TestOracle_CreateGetManyWithFilterAndSorting"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_CreateGetManyWithSortingAndPaging(t *testing.T) {
	name := "TestOracle_CreateGetManyWithSortingAndPaging"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_Update(t *testing.T) {
	name := "TestOracle_Update"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_UpdateNotExist(t *testing.T) {
	name := "TestOracle_UpdateNotExist"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_UpdateDuplicated(t *testing.T) {
	name := "TestOracle_UpdateDuplicated"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_SaveNew(t *testing.T) {
	name := "TestOracle_SaveNew"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_SaveExisting(t *testing.T) {
	name := "TestOracle_SaveExisting"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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

func TestOracle_SaveExistingUnique(t *testing.T) {
	name := "TestOracle_SaveExistingUnique"
	tblName := "table_temp"
	sqlc, dao := _testOracleInit(t, name, tblName)
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
