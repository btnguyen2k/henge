package henge

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	_ "github.com/btnguyen2k/gocosmos"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/sql"
)

var (
	cosmosdbSingleTableBoTypes = []string{"users", "products", "none"}
)

func TestCosmosdbSingleTable_Create(t *testing.T) {
	name := "TestCosmosdbSingleTable_Create"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(colCosmosdbPk, boType)
		ubo.SetExtraAttr("email", "myname@mydomain.com")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh")
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN)")
			ubo.SetDataAttr("name.vi", "Product name (VI)")
			ubo.SetExtraAttr("stock", 35)
		}

		if ok, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", name)
		}
	}
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes), value)
	}
}

func TestCosmosdbSingleTable_CreateExistingPK(t *testing.T) {
	name := "TestCosmosdbSingleTable_CreateExistingPK"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(colCosmosdbPk, boType)
		ubo.SetExtraAttr("email", "myname@mydomain.com")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh")
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN)")
			ubo.SetDataAttr("name.vi", "Product name (VI)")
			ubo.SetExtraAttr("stock", 35)
		}

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
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes), value)
	}
}

func TestCosmosdbSingleTable_CreateExistingUnique(t *testing.T) {
	name := "TestCosmosdbSingleTable_CreateExistingUnique"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(colCosmosdbPk, boType)
		ubo.SetExtraAttr("email", "myname@mydomain.com")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh")
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN)")
			ubo.SetDataAttr("name.vi", "Product name (VI)")
			ubo.SetExtraAttr("stock", 35)
		}

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
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes), value)
	}
}

func TestCosmosdbSingleTable_CreateGet(t *testing.T) {
	name := "TestCosmosdbSingleTable_CreateGet"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(colCosmosdbPk, boType)
		ubo.SetExtraAttr("email", "myname@mydomain.com")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh")
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN)")
			ubo.SetDataAttr("name.vi", "Product name (VI)")
			ubo.SetExtraAttr("stock", 35)
		}

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
			if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "myname@mydomain.com", v)
			}
			if boType == "users" {
				if v := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh", v)
				}
				if v := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
					t.Fatalf("%s failed: expected %#v but received %#v", name, int64(35), v)
				}
			} else if boType == "products" {
				if v := bo.GetDataAttrAsUnsafe("name.en", reddo.TypeString); v != "Product name (EN)" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Product name (EN)", v)
				}
				if v := bo.GetDataAttrAsUnsafe("name.vi", reddo.TypeString); v != "Product name (VI)" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Product name (VI)", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt); v != int64(35) {
					t.Fatalf("%s failed: expected %#v but received %#v", name, int64(35), v)
				}
			}
		}
	}
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes), value)
	}
}

func TestCosmosdbSingleTable_CreateDelete(t *testing.T) {
	name := "TestCosmosdbSingleTable_CreateDelete"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(colCosmosdbPk, boType)
		ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
		ubo.SetExtraAttr("email", "myname@mydomain.com")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh")
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN)")
			ubo.SetDataAttr("name.vi", "Product name (VI)")
			ubo.SetExtraAttr("stock", 35)
		}

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
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != 0 {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, 0, value)
	}
}

func TestCosmosdbSingleTable_CreateGetMany(t *testing.T) {
	name := "TestCosmosdb_CreateGetMany"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		idList := make([]string, 0)
		for i := 0; i < 10; i++ {
			idList = append(idList, strconv.Itoa(i))
		}
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
		for i := 0; i < 10; i++ {
			ubo := NewUniversalBo(idList[i], uint64(i))
			ubo.SetExtraAttr(colCosmosdbPk, boType)
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("name.first", strconv.Itoa(i))
				ubo.SetDataAttr("name.last", "Nguyen")
				ubo.SetExtraAttr("age", 35+i)
			} else if boType == "products" {
				ubo.SetDataAttr("name.en", "Product name (EN)"+strconv.Itoa(i))
				ubo.SetDataAttr("name.vi", "Product name (VI)"+strconv.Itoa(i))
				ubo.SetExtraAttr("stock", 35+i)
			}
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
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes)*10, value)
	}
}

func TestCosmosdbSingleTable_CreateGetManyWithFilter(t *testing.T) {
	name := "TestCosmosdbSingleTable_CreateGetManyWithFilter"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		idList := make([]string, 0)
		for i := 0; i < 10; i++ {
			idList = append(idList, strconv.Itoa(i))
		}
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
		for i := 0; i < 10; i++ {
			ubo := NewUniversalBo(idList[i], uint64(i))
			ubo.SetExtraAttr(colCosmosdbPk, boType)
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("name.first", strconv.Itoa(i))
				ubo.SetDataAttr("name.last", "Nguyen")
				ubo.SetExtraAttr("age", 35+i)
			} else if boType == "products" {
				ubo.SetDataAttr("name.en", "Product name (EN)"+strconv.Itoa(i))
				ubo.SetDataAttr("name.vi", "Product name (VI)"+strconv.Itoa(i))
				ubo.SetExtraAttr("stock", 35+i)
			}
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", name)
			}
		}

		filter := &sql.FilterFieldValue{Field: "email", Operator: ">=", Value: "3@mydomain.com"}
		if boType == "users" {
			filter = &sql.FilterFieldValue{Field: "age", Operator: ">=", Value: 35 + 3}
		} else if boType == "products" {
			filter = &sql.FilterFieldValue{Field: "stock", Operator: ">=", Value: 35 + 3}
		}
		if boList, err := dao.GetAll(filter, nil); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if len(boList) != 7 {
			t.Fatalf("%s failed: expected %#v items but received %#v", name+"/"+boType, 7, len(boList))
		}
	}
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes)*10, value)
	}
}

func TestCosmosdbSingleTable_CreateGetManyWithSorting(t *testing.T) {
	name := "TestCosmosdb_CreateGetManyWithSorting"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		idList := make([]string, 0)
		for i := 0; i < 10; i++ {
			idList = append(idList, strconv.Itoa(i))
		}
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
		for i := 0; i < 10; i++ {
			ubo := NewUniversalBo(idList[i], uint64(i))
			ubo.SetExtraAttr(colCosmosdbPk, boType)
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("name.first", strconv.Itoa(i))
				ubo.SetDataAttr("name.last", "Nguyen")
				ubo.SetExtraAttr("age", 35+i)
			} else if boType == "products" {
				ubo.SetDataAttr("name.en", "Product name (EN)"+strconv.Itoa(i))
				ubo.SetDataAttr("name.vi", "Product name (VI)"+strconv.Itoa(i))
				ubo.SetExtraAttr("stock", 35+i)
			}
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", name)
			}
		}

		sorting := map[string]string{"email": "desc"}
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
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes)*10, value)
	}
}

func TestCosmosdbSingleTable_CreateGetManyWithFilterAndSorting(t *testing.T) {
	name := "TestCosmosdbSingleTable_CreateGetManyWithFilterAndSorting"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		idList := make([]string, 0)
		for i := 0; i < 10; i++ {
			idList = append(idList, strconv.Itoa(i))
		}
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
		for i := 0; i < 10; i++ {
			ubo := NewUniversalBo(idList[i], uint64(i))
			ubo.SetExtraAttr(colCosmosdbPk, boType)
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("name.first", strconv.Itoa(i))
				ubo.SetDataAttr("name.last", "Nguyen")
				ubo.SetExtraAttr("age", 35+i)
			} else if boType == "products" {
				ubo.SetDataAttr("name.en", "Product name (EN)"+strconv.Itoa(i))
				ubo.SetDataAttr("name.vi", "Product name (VI)"+strconv.Itoa(i))
				ubo.SetExtraAttr("stock", 35+i)
			}
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", name)
			}
		}

		filter := &sql.FilterFieldValue{Field: "email", Operator: "<", Value: "3@mydomain.com"}
		sorting := map[string]string{"email": "desc"}
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
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes)*10, value)
	}
}

func TestCosmosdbSingleTable_CreateGetManyWithSortingAndPaging(t *testing.T) {
	name := "TestCosmosdbSingleTable_CreateGetManyWithSortingAndPaging"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		idList := make([]string, 0)
		for i := 0; i < 10; i++ {
			idList = append(idList, strconv.Itoa(i))
		}
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
		for i := 0; i < 10; i++ {
			ubo := NewUniversalBo(idList[i], uint64(i))
			ubo.SetExtraAttr(colCosmosdbPk, boType)
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("name.first", strconv.Itoa(i))
				ubo.SetDataAttr("name.last", "Nguyen")
				ubo.SetExtraAttr("age", 35+i)
			} else if boType == "products" {
				ubo.SetDataAttr("name.en", "Product name (EN)"+strconv.Itoa(i))
				ubo.SetDataAttr("name.vi", "Product name (VI)"+strconv.Itoa(i))
				ubo.SetExtraAttr("stock", 35+i)
			}
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", name)
			}
		}

		fromOffset := 3
		numRows := 4
		sorting := map[string]string{"email": "desc"}
		if boList, err := dao.GetN(fromOffset, numRows, nil, sorting); err != nil {
			t.Fatalf("%s failed: %s", name+"/"+boType, err)
		} else if len(boList) != numRows {
			t.Fatalf("%s failed: expected %#v items but received %#v", name+"/"+boType, numRows, len(boList))
		} else {
			for i := 0; i < numRows; i++ {
				if boList[i].GetId() != strconv.Itoa(9-i-fromOffset) {
					t.Fatalf("%s failed: expected record %#v but received %#v", name+"/"+boType, strconv.Itoa(9-i-fromOffset), boList[i].GetId())
				}
			}
		}
	}
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes)*10, value)
	}
}

func TestCosmosdbSingleTable_Update(t *testing.T) {
	name := "TestCosmosdbSingleTable_Update"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(colCosmosdbPk, boType)
		ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
		ubo.SetExtraAttr("email", "myname@mydomain.com")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh")
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN)")
			ubo.SetDataAttr("name.vi", "Product name (VI)")
			ubo.SetExtraAttr("stock", 35)
		}

		if _, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}

		ubo.SetExtraAttr("email", "thanh@mydomain.com")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh2")
			ubo.SetDataAttr("name.last", "Nguyen2")
			ubo.SetExtraAttr("age", 37)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN2)")
			ubo.SetDataAttr("name.vi", "Product name (VI2)")
			ubo.SetExtraAttr("stock", 37)
		}
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
			if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "thanh@mydomain.com" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "thanh@mydomain.com", v)
			}
			if boType == "users" {
				if v := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh2" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh2", v)
				}
				if v := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen2" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen2", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
					t.Fatalf("%s failed: expected %#v but received %#v", name, int64(37), v)
				}
			} else if boType == "products" {
				if v := bo.GetDataAttrAsUnsafe("name.en", reddo.TypeString); v != "Product name (EN2)" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Product name (EN2)", v)
				}
				if v := bo.GetDataAttrAsUnsafe("name.vi", reddo.TypeString); v != "Product name (VI2)" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Product name (VI2)", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt); v != int64(37) {
					t.Fatalf("%s failed: expected %#v but received %#v", name, int64(37), v)
				}
			}
		}
	}
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes), value)
	}
}

func TestCosmosdbSingleTable_UpdateNotExist(t *testing.T) {
	name := "TestCosmosdbSingleTable_UpdateNotExist"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(colCosmosdbPk, boType)
		ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
		ubo.SetExtraAttr("email", "myname@mydomain.com")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh")
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN)")
			ubo.SetDataAttr("name.vi", "Product name (VI)")
			ubo.SetExtraAttr("stock", 35)
		}

		if ok, err := dao.Update(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if ok {
			t.Fatalf("%s failed: record should not be updated", name)
		}
	}
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != 0 {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, 0, value)
	}
}

func TestCosmosdbSingleTable_UpdateDuplicated(t *testing.T) {
	name := "TestCosmosdbSingleTable_UpdateDuplicated"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		ubo1 := NewUniversalBo("1", 1357)
		ubo1.SetExtraAttr(colCosmosdbPk, boType)
		ubo1.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
		ubo1.SetExtraAttr("email", "1@mydomain.com")
		if boType == "users" {
			ubo1.SetDataAttr("name.first", "Thanh")
			ubo1.SetDataAttr("name.last", "Nguyen")
			ubo1.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo1.SetDataAttr("name.en", "Product name (EN)")
			ubo1.SetDataAttr("name.vi", "Product name (VI)")
			ubo1.SetExtraAttr("stock", 35)
		}
		if _, err := dao.Create(ubo1); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}

		ubo2 := NewUniversalBo("2", 1357)
		ubo2.SetExtraAttr(colCosmosdbPk, boType)
		ubo2.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
		ubo2.SetExtraAttr("email", "2@mydomain.com")
		if boType == "users" {
			ubo1.SetDataAttr("name.first", "Thanh2")
			ubo1.SetDataAttr("name.last", "Nguyen2")
			ubo1.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo1.SetDataAttr("name.en", "Product name (EN2)")
			ubo1.SetDataAttr("name.vi", "Product name (VI2)")
			ubo1.SetExtraAttr("stock", 35)
		}
		if _, err := dao.Create(ubo2); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}

		ubo1.SetExtraAttr("email", "2@mydomain.com")
		if _, err := dao.Update(ubo1); err != godal.GdaoErrorDuplicatedEntry {
			t.Fatalf("%s failed: %s", name, err)
		}
	}
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes)*2 {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes)*2, value)
	}
}

func TestCosmosdbSingleTable_SaveNew(t *testing.T) {
	name := "TestCosmosdbSingleTable_SaveNew"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(colCosmosdbPk, boType)
		ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
		ubo.SetExtraAttr("email", "myname@mydomain.com")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh")
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN)")
			ubo.SetDataAttr("name.vi", "Product name (VI)")
			ubo.SetExtraAttr("stock", 35)
		}
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
			if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "myname@mydomain.com", v)
			}
			if boType == "users" {
				if v := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh", v)
				}
				if v := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
					t.Fatalf("%s failed: expected %#v but received %#v", name, int64(35), v)
				}
			} else if boType == "products" {
				if v := bo.GetDataAttrAsUnsafe("name.en", reddo.TypeString); v != "Product name (EN)" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Product name (EN)", v)
				}
				if v := bo.GetDataAttrAsUnsafe("name.vi", reddo.TypeString); v != "Product name (VI)" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Product name (VI)", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt); v != int64(35) {
					t.Fatalf("%s failed: expected %#v but received %#v", name, int64(35), v)
				}
			}
		}
	}
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes), value)
	}
}

func TestCosmosdbSingleTable_SaveExisting(t *testing.T) {
	name := "TestCosmosdbSingleTable_SaveExisting"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(colCosmosdbPk, boType)
		ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
		ubo.SetExtraAttr("email", "myname@mydomain.com")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh")
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN)")
			ubo.SetDataAttr("name.vi", "Product name (VI)")
			ubo.SetExtraAttr("stock", 35)
		}
		if _, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}

		ubo.SetExtraAttr("email", "thanh@mydomain.com")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh2")
			ubo.SetDataAttr("name.last", "Nguyen2")
			ubo.SetExtraAttr("age", 37)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN2)")
			ubo.SetDataAttr("name.vi", "Product name (VI2)")
			ubo.SetExtraAttr("stock", 37)
		}
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
			if v := old.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "myname@mydomain.com", v)
			}
			if boType == "users" {
				if v := old.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh", v)
				}
				if v := old.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen", v)
				}
				if v := old.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
					t.Fatalf("%s failed: expected %#v but received %#v", name, int64(35), v)
				}
			} else if boType == "products" {
				if v := old.GetDataAttrAsUnsafe("name.en", reddo.TypeString); v != "Product name (EN)" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Product name (EN)", v)
				}
				if v := old.GetDataAttrAsUnsafe("name.vi", reddo.TypeString); v != "Product name (VI)" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Product name (VI)", v)
				}
				if v := old.GetExtraAttrAsUnsafe("stock", reddo.TypeInt); v != int64(35) {
					t.Fatalf("%s failed: expected %#v but received %#v", name, int64(35), v)
				}
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
			if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "thanh@mydomain.com" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "thanh@mydomain.com", v)
			}
			if boType == "users" {
				if v := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh2" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh2", v)
				}
				if v := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen2" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen2", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
					t.Fatalf("%s failed: expected %#v but received %#v", name, int64(37), v)
				}
			} else if boType == "products" {
				if v := bo.GetDataAttrAsUnsafe("name.en", reddo.TypeString); v != "Product name (EN2)" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Product name (EN2)", v)
				}
				if v := bo.GetDataAttrAsUnsafe("name.vi", reddo.TypeString); v != "Product name (VI2)" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Product name (VI2)", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt); v != int64(37) {
					t.Fatalf("%s failed: expected %#v but received %#v", name, int64(37), v)
				}
			}
		}
	}
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes), value)
	}
}

func TestCosmosdbSingleTable_SaveExistingUnique(t *testing.T) {
	name := "TestCosmosdbSingleTable_SaveExistingUnique"
	tblName := "table_temp"
	sqlc := _testCosmosdbInitSqlConnect(t, name, tblName)
	defer sqlc.Close()
	for _, boType := range cosmosdbSingleTableBoTypes {
		dao := _testCosmosdbInit(t, name, sqlc, tblName, colCosmosdbPk, boType)
		ubo1 := NewUniversalBo("1", 1357)
		ubo1.SetExtraAttr(colCosmosdbPk, boType)
		ubo1.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
		ubo1.SetExtraAttr("email", "1@mydomain.com")
		if boType == "users" {
			ubo1.SetDataAttr("name.first", "Thanh")
			ubo1.SetDataAttr("name.last", "Nguyen")
			ubo1.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo1.SetDataAttr("name.en", "Product name (EN)")
			ubo1.SetDataAttr("name.vi", "Product name (VI)")
			ubo1.SetExtraAttr("stock", 35)
		}
		if _, err := dao.Create(ubo1); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}

		ubo2 := NewUniversalBo("2", 1357)
		ubo2.SetExtraAttr(colCosmosdbPk, boType)
		ubo2.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
		ubo2.SetExtraAttr("email", "2@mydomain.com")
		if boType == "users" {
			ubo1.SetDataAttr("name.first", "Thanh2")
			ubo1.SetDataAttr("name.last", "Nguyen2")
			ubo1.SetExtraAttr("age", 35)
		} else if boType == "products" {
			ubo1.SetDataAttr("name.en", "Product name (EN2)")
			ubo1.SetDataAttr("name.vi", "Product name (VI2)")
			ubo1.SetExtraAttr("stock", 35)
		}
		if _, err := dao.Create(ubo2); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}

		ubo1.SetExtraAttr("email", "2@mydomain.com")
		if _, _, err := dao.Save(ubo1); err != godal.GdaoErrorDuplicatedEntry {
			t.Fatalf("%s failed: %s", name, err)
		}
	}
	if dbRows, err := sqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", tblName)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if rows, err := sqlc.FetchRows(dbRows); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes)*2 {
		t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", name, len(cosmosdbSingleTableBoTypes)*2, value)
	}
}
