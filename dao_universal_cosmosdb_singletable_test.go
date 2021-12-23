package henge

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	_ "github.com/btnguyen2k/gocosmos"
	"github.com/btnguyen2k/godal"
)

var (
	cosmosdbSingleTableBoTypes = []string{"users", "products", "none"}
)

func TestUniversalDaoCosmosdbSql_SingleTable_Create(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_Create"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo.SetExtraAttr("email", "myname@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh")
				ubo.SetDataAttr("testName.last", "Nguyen")
				ubo.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI)")
				ubo.SetExtraAttr("stock", 35)
			}

			if ok, err := testDao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", testName)
			}
		})
	}

	// testSqlc = _testCosmosdbInitSqlConnect(t, testName, testTable)
	// defer testSqlc.Close()
	// if dbRows, err := testSqlc.GetDB().Query(fmt.Sprintf("SELECT COUNT(1) FROM %s c WITH cross_partition=true", testTable)); err != nil {
	// 	t.Fatalf("%s failed: %s", testName, err)
	// } else if rows, err := testSqlc.FetchRows(dbRows); err != nil {
	// 	t.Fatalf("%s failed: %s", testName, err)
	// } else if value := rows[0]["$1"]; int(value.(float64)) != len(cosmosdbSingleTableBoTypes) {
	// 	t.Fatalf("%s failed: expected collection to have %#v rows but received %#v", testName, len(cosmosdbSingleTableBoTypes), value)
	// }
}

func TestUniversalDaoCosmosdbSql_SingleTable_CreateExistingPK(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_CreateExistingPK"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo.SetExtraAttr("email", "myname@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh")
				ubo.SetDataAttr("testName.last", "Nguyen")
				ubo.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI)")
				ubo.SetExtraAttr("stock", 35)
			}

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
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_CreateExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_CreateExistingUnique"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo.SetExtraAttr("email", "myname@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh")
				ubo.SetDataAttr("testName.last", "Nguyen")
				ubo.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI)")
				ubo.SetExtraAttr("stock", 35)
			}

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
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_CreateGet(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_CreateGet"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo.SetExtraAttr("email", "myname@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh")
				ubo.SetDataAttr("testName.last", "Nguyen")
				ubo.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI)")
				ubo.SetExtraAttr("stock", 35)
			}

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
				if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
					t.Fatalf("%s failed: expected %#v but received %#v", testName, "myname@mydomain.com", v)
				}
				if boType == "users" {
					if v := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh", v)
					}
					if v := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(35), v)
					}
				} else if boType == "products" {
					if v := bo.GetDataAttrAsUnsafe("testName.en", reddo.TypeString); v != "Product testName (EN)" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Product testName (EN)", v)
					}
					if v := bo.GetDataAttrAsUnsafe("testName.vi", reddo.TypeString); v != "Product testName (VI)" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Product testName (VI)", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt); v != int64(35) {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(35), v)
					}
				}
			}
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_CreateDelete(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_CreateDelete"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
			ubo.SetExtraAttr("email", "myname@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh")
				ubo.SetDataAttr("testName.last", "Nguyen")
				ubo.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI)")
				ubo.SetExtraAttr("stock", 35)
			}

			if ok, err := testDao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", testName)
			}

			if bo, err := testDao.Get("id"); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if bo == nil {
				t.Fatalf("%s failed: not found", testName)
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
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_CreateGetMany(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_CreateGetMany"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			idList := make([]string, 0)
			for i := 0; i < 10; i++ {
				idList = append(idList, strconv.Itoa(i))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
			for i := 0; i < 10; i++ {
				ubo := NewUniversalBo(idList[i], uint64(i))
				ubo.SetExtraAttr(testCosmosdbPkCol, boType)
				ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
				if boType == "users" {
					ubo.SetDataAttr("testName.first", strconv.Itoa(i))
					ubo.SetDataAttr("testName.last", "Nguyen")
					ubo.SetExtraAttr("age", 35+i)
				} else if boType == "products" {
					ubo.SetDataAttr("testName.en", "Product testName (EN)"+strconv.Itoa(i))
					ubo.SetDataAttr("testName.vi", "Product testName (VI)"+strconv.Itoa(i))
					ubo.SetExtraAttr("stock", 35+i)
				}
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
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_CreateGetManyWithFilter(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_CreateGetManyWithFilter"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			idList := make([]string, 0)
			for i := 0; i < 10; i++ {
				idList = append(idList, strconv.Itoa(i))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
			for i := 0; i < 10; i++ {
				ubo := NewUniversalBo(idList[i], uint64(i))
				ubo.SetExtraAttr(testCosmosdbPkCol, boType)
				ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
				if boType == "users" {
					ubo.SetDataAttr("testName.first", strconv.Itoa(i))
					ubo.SetDataAttr("testName.last", "Nguyen")
					ubo.SetExtraAttr("age", 35+i)
				} else if boType == "products" {
					ubo.SetDataAttr("testName.en", "Product testName (EN)"+strconv.Itoa(i))
					ubo.SetDataAttr("testName.vi", "Product testName (VI)"+strconv.Itoa(i))
					ubo.SetExtraAttr("stock", 35+i)
				}
				if ok, err := testDao.Create(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if !ok {
					t.Fatalf("%s failed: cannot create record", testName)
				}
			}

			var filter godal.FilterOpt = &godal.FilterOptFieldOpValue{FieldName: "email", Operator: godal.FilterOpGreaterOrEqual, Value: "3@mydomain.com"}
			if boType == "users" {
				filter = &godal.FilterOptFieldOpValue{FieldName: "age", Operator: godal.FilterOpGreaterOrEqual, Value: 35 + 3}
			} else if boType == "products" {
				filter = &godal.FilterOptFieldOpValue{FieldName: "stock", Operator: godal.FilterOpGreaterOrEqual, Value: 35 + 3}
			}
			if boList, err := testDao.GetAll(filter, nil); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if len(boList) != 7 {
				t.Fatalf("%s failed: expected %#v items but received %#v", testName+"/"+boType, 7, len(boList))
			}
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_CreateGetManyWithSorting(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_CreateGetManyWithSorting"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			idList := make([]string, 0)
			for i := 0; i < 10; i++ {
				idList = append(idList, strconv.Itoa(i))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
			for i := 0; i < 10; i++ {
				ubo := NewUniversalBo(idList[i], uint64(i))
				ubo.SetExtraAttr(testCosmosdbPkCol, boType)
				ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
				if boType == "users" {
					ubo.SetDataAttr("testName.first", strconv.Itoa(i))
					ubo.SetDataAttr("testName.last", "Nguyen")
					ubo.SetExtraAttr("age", 35+i)
				} else if boType == "products" {
					ubo.SetDataAttr("testName.en", "Product testName (EN)"+strconv.Itoa(i))
					ubo.SetDataAttr("testName.vi", "Product testName (VI)"+strconv.Itoa(i))
					ubo.SetExtraAttr("stock", 35+i)
				}
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
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_CreateGetManyWithFilterAndSorting(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_CreateGetManyWithFilterAndSorting"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			idList := make([]string, 0)
			for i := 0; i < 10; i++ {
				idList = append(idList, strconv.Itoa(i))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
			for i := 0; i < 10; i++ {
				ubo := NewUniversalBo(idList[i], uint64(i))
				ubo.SetExtraAttr(testCosmosdbPkCol, boType)
				ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
				if boType == "users" {
					ubo.SetDataAttr("testName.first", strconv.Itoa(i))
					ubo.SetDataAttr("testName.last", "Nguyen")
					ubo.SetExtraAttr("age", 35+i)
				} else if boType == "products" {
					ubo.SetDataAttr("testName.en", "Product testName (EN)"+strconv.Itoa(i))
					ubo.SetDataAttr("testName.vi", "Product testName (VI)"+strconv.Itoa(i))
					ubo.SetExtraAttr("stock", 35+i)
				}
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
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_CreateGetManyWithSortingAndPaging(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_CreateGetManyWithSortingAndPaging"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			idList := make([]string, 0)
			for i := 0; i < 10; i++ {
				idList = append(idList, strconv.Itoa(i))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
			for i := 0; i < 10; i++ {
				ubo := NewUniversalBo(idList[i], uint64(i))
				ubo.SetExtraAttr(testCosmosdbPkCol, boType)
				ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
				if boType == "users" {
					ubo.SetDataAttr("testName.first", strconv.Itoa(i))
					ubo.SetDataAttr("testName.last", "Nguyen")
					ubo.SetExtraAttr("age", 35+i)
				} else if boType == "products" {
					ubo.SetDataAttr("testName.en", "Product testName (EN)"+strconv.Itoa(i))
					ubo.SetDataAttr("testName.vi", "Product testName (VI)"+strconv.Itoa(i))
					ubo.SetExtraAttr("stock", 35+i)
				}
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
				t.Fatalf("%s failed: %s", testName+"/"+boType, err)
			} else if len(boList) != numRows {
				t.Fatalf("%s failed: expected %#v items but received %#v", testName+"/"+boType, numRows, len(boList))
			} else {
				for i := 0; i < numRows; i++ {
					if boList[i].GetId() != strconv.Itoa(9-i-fromOffset) {
						t.Fatalf("%s failed: expected record %#v but received %#v", testName+"/"+boType, strconv.Itoa(9-i-fromOffset), boList[i].GetId())
					}
				}
			}
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_Update(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_Update"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
			ubo.SetExtraAttr("email", "myname@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh")
				ubo.SetDataAttr("testName.last", "Nguyen")
				ubo.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI)")
				ubo.SetExtraAttr("stock", 35)
			}

			if _, err := testDao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}

			ubo.SetExtraAttr("email", "thanh@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh2")
				ubo.SetDataAttr("testName.last", "Nguyen2")
				ubo.SetExtraAttr("age", 37)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN2)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI2)")
				ubo.SetExtraAttr("stock", 37)
			}
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
				if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "thanh@mydomain.com" {
					t.Fatalf("%s failed: expected %#v but received %#v", testName, "thanh@mydomain.com", v)
				}
				if boType == "users" {
					if v := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh2" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh2", v)
					}
					if v := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen2" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen2", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(37), v)
					}
				} else if boType == "products" {
					if v := bo.GetDataAttrAsUnsafe("testName.en", reddo.TypeString); v != "Product testName (EN2)" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Product testName (EN2)", v)
					}
					if v := bo.GetDataAttrAsUnsafe("testName.vi", reddo.TypeString); v != "Product testName (VI2)" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Product testName (VI2)", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt); v != int64(37) {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(37), v)
					}
				}
			}
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_UpdateNotExist(t *testing.T) {
	testName := "TestCosmosdbSingleTable_UpdateNotExist"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
			ubo.SetExtraAttr("email", "myname@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh")
				ubo.SetDataAttr("testName.last", "Nguyen")
				ubo.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI)")
				ubo.SetExtraAttr("stock", 35)
			}

			if ok, err := testDao.Update(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if ok {
				t.Fatalf("%s failed: record should not be updated", testName)
			}
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_UpdateDuplicated(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_UpdateDuplicated"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			ubo1 := NewUniversalBo("1", 1357)
			ubo1.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo1.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
			ubo1.SetExtraAttr("email", "1@mydomain.com")
			if boType == "users" {
				ubo1.SetDataAttr("testName.first", "Thanh")
				ubo1.SetDataAttr("testName.last", "Nguyen")
				ubo1.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo1.SetDataAttr("testName.en", "Product testName (EN)")
				ubo1.SetDataAttr("testName.vi", "Product testName (VI)")
				ubo1.SetExtraAttr("stock", 35)
			}
			if _, err := testDao.Create(ubo1); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}

			ubo2 := NewUniversalBo("2", 1357)
			ubo2.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo2.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
			ubo2.SetExtraAttr("email", "2@mydomain.com")
			if boType == "users" {
				ubo1.SetDataAttr("testName.first", "Thanh2")
				ubo1.SetDataAttr("testName.last", "Nguyen2")
				ubo1.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo1.SetDataAttr("testName.en", "Product testName (EN2)")
				ubo1.SetDataAttr("testName.vi", "Product testName (VI2)")
				ubo1.SetExtraAttr("stock", 35)
			}
			if _, err := testDao.Create(ubo2); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}

			ubo1.SetExtraAttr("email", "2@mydomain.com")
			if _, err := testDao.Update(ubo1); err != godal.ErrGdaoDuplicatedEntry {
				t.Fatalf("%s failed: %s", testName, err)
			}
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_SaveNew(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_SaveNew"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
			ubo.SetExtraAttr("email", "myname@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh")
				ubo.SetDataAttr("testName.last", "Nguyen")
				ubo.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI)")
				ubo.SetExtraAttr("stock", 35)
			}
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
				if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
					t.Fatalf("%s failed: expected %#v but received %#v", testName, "myname@mydomain.com", v)
				}
				if boType == "users" {
					if v := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh", v)
					}
					if v := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(35), v)
					}
				} else if boType == "products" {
					if v := bo.GetDataAttrAsUnsafe("testName.en", reddo.TypeString); v != "Product testName (EN)" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Product testName (EN)", v)
					}
					if v := bo.GetDataAttrAsUnsafe("testName.vi", reddo.TypeString); v != "Product testName (VI)" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Product testName (VI)", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt); v != int64(35) {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(35), v)
					}
				}
			}
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_SaveExisting(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_SaveExisting"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
			ubo.SetExtraAttr("email", "myname@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh")
				ubo.SetDataAttr("testName.last", "Nguyen")
				ubo.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI)")
				ubo.SetExtraAttr("stock", 35)
			}
			if _, err := testDao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}

			ubo.SetExtraAttr("email", "thanh@mydomain.com")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh2")
				ubo.SetDataAttr("testName.last", "Nguyen2")
				ubo.SetExtraAttr("age", 37)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN2)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI2)")
				ubo.SetExtraAttr("stock", 37)
			}
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
				if v := old.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
					t.Fatalf("%s failed: expected %#v but received %#v", testName, "myname@mydomain.com", v)
				}
				if boType == "users" {
					if v := old.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh", v)
					}
					if v := old.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen", v)
					}
					if v := old.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(35), v)
					}
				} else if boType == "products" {
					if v := old.GetDataAttrAsUnsafe("testName.en", reddo.TypeString); v != "Product testName (EN)" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Product testName (EN)", v)
					}
					if v := old.GetDataAttrAsUnsafe("testName.vi", reddo.TypeString); v != "Product testName (VI)" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Product testName (VI)", v)
					}
					if v := old.GetExtraAttrAsUnsafe("stock", reddo.TypeInt); v != int64(35) {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(35), v)
					}
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
				if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "thanh@mydomain.com" {
					t.Fatalf("%s failed: expected %#v but received %#v", testName, "thanh@mydomain.com", v)
				}
				if boType == "users" {
					if v := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh2" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh2", v)
					}
					if v := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen2" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen2", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(37), v)
					}
				} else if boType == "products" {
					if v := bo.GetDataAttrAsUnsafe("testName.en", reddo.TypeString); v != "Product testName (EN2)" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Product testName (EN2)", v)
					}
					if v := bo.GetDataAttrAsUnsafe("testName.vi", reddo.TypeString); v != "Product testName (VI2)" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Product testName (VI2)", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt); v != int64(37) {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(37), v)
					}
				}
			}
		})
	}
}

func TestUniversalDaoCosmosdbSql_SingleTable_SaveExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoCosmosdbSql_SingleTable_SaveExistingUnique"
	for _, boType := range cosmosdbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			teardownTest := setupTest(t, testName, setupTestCosmosdb, teardownTestCosmosdb)
			defer teardownTest(t)
			_dao := testDao.(*UniversalDaoCosmosdbSql)
			_dao.pkValue = boType

			ubo1 := NewUniversalBo("1", 1357)
			ubo1.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo1.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
			ubo1.SetExtraAttr("email", "1@mydomain.com")
			if boType == "users" {
				ubo1.SetDataAttr("testName.first", "Thanh")
				ubo1.SetDataAttr("testName.last", "Nguyen")
				ubo1.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo1.SetDataAttr("testName.en", "Product testName (EN)")
				ubo1.SetDataAttr("testName.vi", "Product testName (VI)")
				ubo1.SetExtraAttr("stock", 35)
			}
			if _, err := testDao.Create(ubo1); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}

			ubo2 := NewUniversalBo("2", 1357)
			ubo2.SetExtraAttr(testCosmosdbPkCol, boType)
			ubo2.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
			ubo2.SetExtraAttr("email", "2@mydomain.com")
			if boType == "users" {
				ubo1.SetDataAttr("testName.first", "Thanh2")
				ubo1.SetDataAttr("testName.last", "Nguyen2")
				ubo1.SetExtraAttr("age", 35)
			} else if boType == "products" {
				ubo1.SetDataAttr("testName.en", "Product testName (EN2)")
				ubo1.SetDataAttr("testName.vi", "Product testName (VI2)")
				ubo1.SetExtraAttr("stock", 35)
			}
			if _, err := testDao.Create(ubo2); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}

			ubo1.SetExtraAttr("email", "2@mydomain.com")
			if _, _, err := testDao.Save(ubo1); err != godal.ErrGdaoDuplicatedEntry {
				t.Fatalf("%s failed: %s", testName, err)
			}
		})
	}
}
