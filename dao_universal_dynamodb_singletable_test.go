package henge

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	prom "github.com/btnguyen2k/prom/dynamodb"
)

const (
	dynamodbSingleTablePkPrefix = "pk"
)

var (
	dynamodbSingleTableBoTypes = []string{"users", "products", "none"}
)

func _testDynamodbSingleTableInit(t *testing.T, testName string, adc *prom.AwsDynamodbConnect, tableName, pkPrefix, pkPrefixValue string, uidxIndexes [][]string) *UniversalDaoDynamodb {
	spec := &DynamodbTablesSpec{
		MainTableRcu: awsDynamodbRCU, MainTableWcu: awsDynamodbWCU,
		CreateUidxTable: true, UidxTableRcu: awsDynamodbRCU, UidxTableWcu: awsDynamodbWCU,
	}
	if pkPrefix != "" {
		spec.MainTableCustomAttrs = []prom.AwsDynamodbNameAndType{{Name: pkPrefix, Type: prom.AwsAttrTypeString}}
		spec.MainTablePkPrefix = pkPrefix
	}
	if err := InitDynamodbTables(adc, tableName, spec); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	daoSpec := &DynamodbDaoSpec{PkPrefix: pkPrefix, PkPrefixValue: pkPrefixValue, UidxAttrs: uidxIndexes}
	return NewUniversalDaoDynamodb(adc, tableName, daoSpec)
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestUniversalDaoDynamodb_SingleTable_Create(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_Create"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

			for _, dao := range []UniversalDao{dao1, dao2} {
				if ok, err := dao.Create(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if !ok {
					t.Fatalf("%s failed: cannot create record", testName)
				}
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// duplicated PK {dynamodbSingleTablePkPrefix, FieldId} should not be allowed.
func TestUniversalDaoDynamodb_SingleTable_CreateExistingPK(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_CreateExistingPK"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

			for _, dao := range []UniversalDao{dao1, dao2} {
				if ok, err := dao.Create(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if !ok {
					t.Fatalf("%s failed: cannot create record", testName)
				}
			}

			ubo.SetExtraAttr("email", "myname2@mydomain.com")
			for _, dao := range []UniversalDao{dao1, dao2} {
				if ok, err := dao.Create(ubo); err != godal.ErrGdaoDuplicatedEntry {
					t.Fatalf("%s failed: %s", testName, err)
				} else if ok {
					t.Fatalf("%s failed: record should not be created twice", testName)
				}
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// duplicated unique key {dynamodbSingleTablePkPrefix, "email"} or {dynamodbSingleTablePkPrefix, "subject", "level"} should not be allowed.
func TestUniversalDaoDynamodb_SingleTable_CreateExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_CreateExistingUnique"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

			for _, dao := range []UniversalDao{dao1, dao2} {
				if ok, err := dao.Create(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if !ok {
					t.Fatalf("%s failed: cannot create record", testName)
				}
			}

			ubo.SetId("id2").SetExtraAttr("subject", "English2").SetExtraAttr("level", "entry2")
			if ok, err := dao1.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", testName)
			}
			if ok, err := dao2.Create(ubo); err != godal.ErrGdaoDuplicatedEntry {
				// duplicated "email"
				t.Fatalf("%s failed: %s", testName, err)
			} else if ok {
				t.Fatalf("%s failed: record should not be created twice", testName)
			}

			ubo.SetId("id3").SetExtraAttr("email", "another@mydomain.com").
				SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
			if ok, err := dao1.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", testName)
			}
			if ok, err := dao2.Create(ubo); err != godal.ErrGdaoDuplicatedEntry {
				// duplicated {"subject","level"}
				t.Fatalf("%s failed: %s", testName, err)
			} else if ok {
				t.Fatalf("%s failed: record should not be created twice", testName)
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*3 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*3, len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestUniversalDaoDynamodb_SingleTable_CreateGet(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_CreateGet"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

			for _, dao := range []UniversalDao{dao1, dao2} {
				if ok, err := dao.Create(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if !ok {
					t.Fatalf("%s failed: cannot create record", testName)
				}

				if bo, err := dao.Get("id"); err != nil {
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
					if v := bo.GetExtraAttrAsUnsafe("subject", reddo.TypeString); v != "English" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "English", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("level", reddo.TypeString); v != "entry" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "entry", v)
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
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestUniversalDaoDynamodb_SingleTable_CreateDelete(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_CreateDelete"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

			for _, dao := range []UniversalDao{dao1, dao2} {
				if ok, err := dao.Create(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if !ok {
					t.Fatalf("%s failed: cannot create record", testName)
				}

				if ok, err := dao.Delete(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if !ok {
					t.Fatalf("%s failed: cannot delete record", testName)
				}

				if bo, err := dao.Get("id"); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if bo != nil {
					t.Fatalf("%s failed: record should be deleted", testName)
				}

				if ok, err := dao.Delete(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if ok {
					t.Fatalf("%s failed: record should not be deleted twice", testName)
				}
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != 0 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, 0, len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != 0 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, 0, len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestUniversalDaoDynamodb_SingleTable_CreateGetMany(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_CreateGetMany"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			idList := make([]string, 0)
			for i := 0; i < 10; i++ {
				idList = append(idList, strconv.Itoa(i))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
			for _, dao := range []UniversalDao{dao1, dao2} {
				for i := 0; i < 10; i++ {
					ubo := NewUniversalBo(idList[i], uint64(i))
					ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
					ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
					ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
					if boType == "users" {
						ubo.SetDataAttr("testName.first", strconv.Itoa(i))
						ubo.SetDataAttr("testName.last", "Nguyen")
						ubo.SetExtraAttr("age", 35+i)
					} else if boType == "products" {
						ubo.SetDataAttr("testName.en", strconv.Itoa(i)+" (EN)")
						ubo.SetDataAttr("testName.vi", strconv.Itoa(i)+" (VI)")
						ubo.SetExtraAttr("stock", 35+i)
					}
					if ok, err := dao.Create(ubo); err != nil {
						t.Fatalf("%s failed: %s", testName, err)
					} else if !ok {
						t.Fatalf("%s failed: cannot create record", testName)
					}
				}

				if boList, err := dao.GetAll(nil, nil); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if len(boList) != 10 {
					t.Fatalf("%s failed: expected %#v items but received %#v", testName+"/"+boType, 10, len(boList))
				}
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestUniversalDaoDynamodb_SingleTable_CreateGetManyWithFilter(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_CreateGetManyWithFilter"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			idList := make([]string, 0)
			for i := 0; i < 10; i++ {
				idList = append(idList, strconv.Itoa(i))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
			for _, dao := range []UniversalDao{dao1, dao2} {
				for i := 0; i < 10; i++ {
					ubo := NewUniversalBo(idList[i], uint64(i))
					ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
					ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
					ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
					if boType == "users" {
						ubo.SetDataAttr("testName.first", strconv.Itoa(i))
						ubo.SetDataAttr("testName.last", "Nguyen")
						ubo.SetExtraAttr("age", 35+i)
					} else if boType == "products" {
						ubo.SetDataAttr("testName.en", strconv.Itoa(i)+" (EN)")
						ubo.SetDataAttr("testName.vi", strconv.Itoa(i)+" (VI)")
						ubo.SetExtraAttr("stock", 35+i)
					}
					if ok, err := dao.Create(ubo); err != nil {
						t.Fatalf("%s failed: %s", testName, err)
					} else if !ok {
						t.Fatalf("%s failed: cannot create record", testName)
					}
				}

				filter := &godal.FilterOptFieldOpValue{FieldName: "email", Operator: godal.FilterOpGreaterOrEqual, Value: "3@mydomain.com"}
				if boType == "users" {
					filter.FieldName, filter.Value = "age", 35+3
				} else if boType == "products" {
					filter.FieldName, filter.Value = "stock", 35+3
				}
				if boList, err := dao.GetAll(filter, nil); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if len(boList) != 7 {
					t.Fatalf("%s failed: expected %#v items but received %#v", testName, 7, len(boList))
				}
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
}

// AWS Dynamodb does not support custom sorting yet
func TestUniversalDaoDynamodb_SingleTable_CreateGetManyWithSorting(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_CreateGetManyWithSorting"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			gsiName := "gsi_email"
			sortField := "email"
			attrsDef := []prom.AwsDynamodbNameAndType{{Name: dynamodbSingleTablePkPrefix, Type: prom.AwsAttrTypeString}, {Name: sortField, Type: prom.AwsAttrTypeString}}
			keyAttrs := []prom.AwsDynamodbNameAndType{
				{Name: dynamodbSingleTablePkPrefix, Type: prom.AwsKeyTypePartition}, {Name: sortField, Type: prom.AwsKeyTypeSort}}
			testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableNoUidx, gsiName, 1, 1, attrsDef, keyAttrs)
			testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableUidx, gsiName, 1, 1, attrsDef, keyAttrs)

			dao1.MapGsi(gsiName, sortField)
			dao2.MapGsi(gsiName, sortField)

			idList := make([]string, 0)
			for i := 0; i < 10; i++ {
				idList = append(idList, strconv.Itoa(i))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
			for _, dao := range []UniversalDao{dao1, dao2} {
				for i := 0; i < 10; i++ {
					ubo := NewUniversalBo(idList[i], uint64(i))
					ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
					ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
					ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
					if boType == "users" {
						ubo.SetDataAttr("testName.first", strconv.Itoa(i))
						ubo.SetDataAttr("testName.last", "Nguyen")
						ubo.SetExtraAttr("age", 35+i)
					} else if boType == "products" {
						ubo.SetDataAttr("testName.en", strconv.Itoa(i)+" (EN)")
						ubo.SetDataAttr("testName.vi", strconv.Itoa(i)+" (VI)")
						ubo.SetExtraAttr("stock", 35+i)
					}
					if ok, err := dao.Create(ubo); err != nil {
						t.Fatalf("%s failed: %s", testName, err)
					} else if !ok {
						t.Fatalf("%s failed: cannot create record", testName)
					}
				}

				sorting := (&godal.SortingField{FieldName: sortField}).ToSortingOpt()
				if boList, err := dao.GetAll(nil, sorting); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if len(boList) != len(idList) {
					t.Fatalf("%s failed: expected %d items but received %d", testName, len(idList), len(boList))
				} else {
					for i := 0; i < 10; i++ {
						if boList[i].GetId() != strconv.Itoa(i) {
							t.Fatalf("%s failed: expected record %#v but received %#v", testName, strconv.Itoa(i), boList[i].GetId())
						}
					}
				}

				sorting = (&godal.SortingField{FieldName: sortField, Descending: true}).ToSortingOpt()
				if boList, err := dao.GetAll(nil, sorting); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if len(boList) != len(idList) {
					t.Fatalf("%s failed: expected %d items but received %d", testName, len(idList), len(boList))
				} else {
					for i := 0; i < 10; i++ {
						if boList[i].GetId() != strconv.Itoa(10-i-1) {
							t.Fatalf("%s failed: expected record %#v but received %#v", testName, strconv.Itoa(10-i-1), boList[i].GetId())
						}
					}
				}
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
}

// AWS Dynamodb does not support custom sorting yet
func TestUniversalDaoDynamodb_SingleTable_CreateGetManyWithFilterAndSorting(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_CreateGetManyWithFilterAndSorting"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			gsiName := "gsi_email"
			sortField := "email"
			attrsDef := []prom.AwsDynamodbNameAndType{{Name: dynamodbSingleTablePkPrefix, Type: prom.AwsAttrTypeString}, {Name: sortField, Type: prom.AwsAttrTypeString}}
			keyAttrs := []prom.AwsDynamodbNameAndType{
				{Name: dynamodbSingleTablePkPrefix, Type: prom.AwsKeyTypePartition}, {Name: sortField, Type: prom.AwsKeyTypeSort}}
			testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableNoUidx, gsiName, 1, 1, attrsDef, keyAttrs)
			testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableUidx, gsiName, 1, 1, attrsDef, keyAttrs)

			dao1.MapGsi(gsiName, sortField)
			dao2.MapGsi(gsiName, sortField)

			idList := make([]string, 0)
			for i := 0; i < 10; i++ {
				idList = append(idList, strconv.Itoa(i))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
			for _, dao := range []UniversalDao{dao1, dao2} {
				for i := 0; i < 10; i++ {
					ubo := NewUniversalBo(idList[i], uint64(i))
					ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
					ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
					ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
					if boType == "users" {
						ubo.SetDataAttr("testName.first", strconv.Itoa(i))
						ubo.SetDataAttr("testName.last", "Nguyen")
						ubo.SetExtraAttr("age", 35+i)
					} else if boType == "products" {
						ubo.SetDataAttr("testName.en", strconv.Itoa(i)+" (EN)")
						ubo.SetDataAttr("testName.vi", strconv.Itoa(i)+" (VI)")
						ubo.SetExtraAttr("stock", 35+i)
					}
					if ok, err := dao.Create(ubo); err != nil {
						t.Fatalf("%s failed: %s", testName, err)
					} else if !ok {
						t.Fatalf("%s failed: cannot create record", testName)
					}
				}

				filter := &godal.FilterOptFieldOpValue{FieldName: "email", Operator: godal.FilterOpLess, Value: "3@mydomain.com"}
				sorting := (&godal.SortingField{FieldName: sortField}).ToSortingOpt()
				if boList, err := dao.GetAll(filter, sorting); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if len(boList) != 3 {
					t.Fatalf("%s failed: expected %#v items but received %#v", testName, 3, len(boList))
				} else {
					if boList[0].GetId() != "0" || boList[1].GetId() != "1" || boList[2].GetId() != "2" {
						t.Fatalf("%s failed", testName)
					}
				}

				sorting = (&godal.SortingField{FieldName: sortField, Descending: true}).ToSortingOpt()
				if boList, err := dao.GetAll(filter, sorting); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if len(boList) != 3 {
					t.Fatalf("%s failed: expected %#v items but received %#v", testName, 3, len(boList))
				} else {
					if boList[0].GetId() != "2" || boList[1].GetId() != "1" || boList[2].GetId() != "0" {
						t.Fatalf("%s failed", testName)
					}
				}
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
}

// AWS Dynamodb does not support custom sorting yet
func TestUniversalDaoDynamodb_SingleTable_CreateGetManyWithSortingAndPaging(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_CreateGetManyWithSortingAndPaging"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			gsiName := "gsi_email"
			sortField := "email"
			attrsDef := []prom.AwsDynamodbNameAndType{{Name: dynamodbSingleTablePkPrefix, Type: prom.AwsAttrTypeString}, {Name: sortField, Type: prom.AwsAttrTypeString}}
			keyAttrs := []prom.AwsDynamodbNameAndType{
				{Name: dynamodbSingleTablePkPrefix, Type: prom.AwsKeyTypePartition}, {Name: sortField, Type: prom.AwsKeyTypeSort}}
			testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableNoUidx, gsiName, 1, 1, attrsDef, keyAttrs)
			testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableUidx, gsiName, 1, 1, attrsDef, keyAttrs)

			dao1.MapGsi(gsiName, sortField)
			dao2.MapGsi(gsiName, sortField)

			idList := make([]string, 0)
			for i := 0; i < 10; i++ {
				idList = append(idList, strconv.Itoa(i))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
			for _, dao := range []UniversalDao{dao1, dao2} {
				for i := 0; i < 10; i++ {
					ubo := NewUniversalBo(idList[i], uint64(i))
					ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
					ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
					ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
					if boType == "users" {
						ubo.SetDataAttr("testName.first", strconv.Itoa(i))
						ubo.SetDataAttr("testName.last", "Nguyen")
						ubo.SetExtraAttr("age", 35+i)
					} else if boType == "products" {
						ubo.SetDataAttr("testName.en", strconv.Itoa(i)+" (EN)")
						ubo.SetDataAttr("testName.vi", strconv.Itoa(i)+" (VI)")
						ubo.SetExtraAttr("stock", 35+i)
					}
					if ok, err := dao.Create(ubo); err != nil {
						t.Fatalf("%s failed: %s", testName, err)
					} else if !ok {
						t.Fatalf("%s failed: cannot create record", testName)
					}
				}

				fromOffset := 3
				numRows := 4

				sorting := (&godal.SortingField{FieldName: sortField}).ToSortingOpt()
				if boList, err := dao.GetN(fromOffset, numRows, nil, sorting); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if len(boList) != numRows {
					t.Fatalf("%s failed: expected %d items but received %d", testName, numRows, len(boList))
				} else {
					for i := 0; i < numRows; i++ {
						if boList[i].GetId() != strconv.Itoa(fromOffset+i) {
							t.Fatalf("%s failed: expected record %#v but received %#v", testName, strconv.Itoa(fromOffset+i), boList[i].GetId())
						}
					}
				}

				sorting = (&godal.SortingField{FieldName: sortField, Descending: true}).ToSortingOpt()
				if boList, err := dao.GetN(fromOffset, numRows, nil, sorting); err != nil {
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
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestUniversalDaoDynamodb_SingleTable_CreateGetManyWithPaging(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_CreateGetManyWithPaging"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			idList := make([]string, 0)
			for i := 0; i < 10; i++ {
				idList = append(idList, strconv.Itoa(i))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
			for _, dao := range []UniversalDao{dao1, dao2} {
				for i := 0; i < 10; i++ {
					ubo := NewUniversalBo(idList[i], uint64(i))
					ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
					ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
					ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
					if boType == "users" {
						ubo.SetDataAttr("testName.first", strconv.Itoa(i))
						ubo.SetDataAttr("testName.last", "Nguyen")
						ubo.SetExtraAttr("age", 35+i)
					} else if boType == "products" {
						ubo.SetDataAttr("testName.en", strconv.Itoa(i)+" (EN)")
						ubo.SetDataAttr("testName.vi", strconv.Itoa(i)+" (VI)")
						ubo.SetExtraAttr("stock", 35+i)
					}
					if ok, err := dao.Create(ubo); err != nil {
						t.Fatalf("%s failed: %s", testName, err)
					} else if !ok {
						t.Fatalf("%s failed: cannot create record", testName)
					}
				}

				fromOffset := 3
				numRows := 4
				filter := &godal.FilterOptFieldOpValue{FieldName: "email", Operator: godal.FilterOpGreaterOrEqual, Value: "3@mydomain.com"}
				if boType == "users" {
					filter.FieldName, filter.Value = "age", 35+3
				} else if boType == "products" {
					filter.FieldName, filter.Value = "stock", 35+3
				}
				if boList, err := dao.GetN(fromOffset, numRows, filter, nil); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if len(boList) != numRows {
					t.Fatalf("%s failed: expected %#v items but received %#v", testName, numRows, len(boList))
				} else {
					for _, bo := range boList {
						if boType == "users" && bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt).(int64) < 35+3 {
							t.Fatalf("%s failed: expected value >= %#v but received %#v", testName+"/"+boType, 35+3, bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt))
						} else if boType == "products" && bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt).(int64) < 35+3 {
							t.Fatalf("%s failed: expected value >= %#v but received %#v", testName+"/"+boType, 35+3, bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt))
						} else if bo.GetExtraAttrAsUnsafe("email", reddo.TypeString).(string) < "3@mydomain.com" {
							t.Fatalf("%s failed: expected value >= %#v but received %#v", testName+"/"+boType, "3@mydomain.com", bo.GetExtraAttrAsUnsafe("email", reddo.TypeString))
						}
					}
				}
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestUniversalDaoDynamodb_SingleTable_Update(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_Update"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

			for _, dao := range []UniversalDao{dao1, dao2} {
				if _, err := dao.Create(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				}
			}

			ubo.SetExtraAttr("email", "thanh@mydomain.com")
			ubo.SetExtraAttr("subject", "Maths").SetExtraAttr("level", "advanced")
			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh2")
				ubo.SetDataAttr("testName.last", "Nguyen2")
				ubo.SetExtraAttr("age", 37)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN2)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI2)")
				ubo.SetExtraAttr("stock", 37)
			}

			for _, dao := range []UniversalDao{dao1, dao2} {
				if ok, err := dao.Update(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if !ok {
					t.Fatalf("%s failed: cannot update record", testName)
				}

				if bo, err := dao.Get("id"); err != nil {
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
					if v := bo.GetExtraAttrAsUnsafe("subject", reddo.TypeString); v != "Maths" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "Maths", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("level", reddo.TypeString); v != "advanced" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "advanced", v)
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
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestUniversalDaoDynamodb_SingleTable_UpdateNotExist(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_UpdateNotExist"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

			for _, dao := range []UniversalDao{dao1, dao2} {
				if ok, err := dao.Update(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if ok {
					t.Fatalf("%s failed: record should not be updated", testName)
				}
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != 0 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, 0, len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != 0 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, 0, len(items))
	}
}

// duplicated unique key {dynamodbSingleTablePkPrefix, "email"} or {dynamodbSingleTablePkPrefix, "subject", "level"} should not be allowed.
func TestUniversalDaoDynamodb_SingleTable_UpdateDuplicated(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_UpdateDuplicated"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			ubo1 := NewUniversalBo("1", 1357)
			ubo2 := NewUniversalBo("2", 1357)
			ubo1.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
			ubo2.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
			for i, ubo := range []*UniversalBo{ubo1, ubo2} {
				idStr := strconv.Itoa(i + 1)
				ubo.SetExtraAttr("email", idStr+"@mydomain.com")
				ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idStr)

				if boType == "users" {
					ubo.SetDataAttr("testName.first", "Name-"+idStr)
					ubo.SetDataAttr("testName.last", "Nguyen")
					ubo.SetExtraAttr("age", 35+i)
				} else if boType == "products" {
					ubo.SetDataAttr("testName.en", "Product testName (EN) - "+idStr)
					ubo.SetDataAttr("testName.vi", "Product testName (VI) - "+idStr)
					ubo.SetExtraAttr("stock", 35+i)
				}
				for _, dao := range []UniversalDao{dao1, dao2} {
					if _, err := dao.Create(ubo); err != nil {
						t.Fatalf("%s failed: %s", testName, err)
					}
				}
			}

			ubo1.SetExtraAttr("email", "2@mydomain.com")
			if _, err := dao1.Update(ubo1); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}
			if _, err := dao2.Update(ubo1); err != godal.ErrGdaoDuplicatedEntry {
				// duplicated email
				t.Fatalf("%s failed: %s", testName, err)
			}

			ubo1.SetExtraAttr("email", "1@mydomain.com")
			ubo1.SetExtraAttr("level", "2")
			if _, err := dao1.Update(ubo1); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}
			if _, err := dao2.Update(ubo1); err != godal.ErrGdaoDuplicatedEntry {
				// duplicated {subject:level}
				t.Fatalf("%s failed: %s", testName, err)
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*2 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*2, len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*2 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*2, len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestUniversalDaoDynamodb_SingleTable_SaveNew(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_SaveNew"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

			for _, dao := range []UniversalDao{dao1, dao2} {
				if ok, old, err := dao.Save(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				} else if !ok {
					t.Fatalf("%s failed: cannot save record", testName)
				} else if old != nil {
					t.Fatalf("%s failed: there should be no existing record", testName)
				}

				if bo, err := dao.Get("id"); err != nil {
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
					if v := bo.GetExtraAttrAsUnsafe("subject", reddo.TypeString); v != "English" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "English", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("level", reddo.TypeString); v != "entry" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "entry", v)
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
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestUniversalDaoDynamodb_SingleTable_SaveExisting(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_SaveExisting"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			ubo := NewUniversalBo("id", 1357)
			ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

			for _, dao := range []UniversalDao{dao1, dao2} {
				if _, err := dao.Create(ubo); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				}
			}

			if boType == "users" {
				ubo.SetDataAttr("testName.first", "Thanh2")
				ubo.SetDataAttr("testName.last", "Nguyen2")
				ubo.SetExtraAttr("age", 37)
			} else if boType == "products" {
				ubo.SetDataAttr("testName.en", "Product testName (EN2)")
				ubo.SetDataAttr("testName.vi", "Product testName (VI2)")
				ubo.SetExtraAttr("stock", 37)
			}
			for _, dao := range []UniversalDao{dao1, dao2} {
				if ok, old, err := dao.Save(ubo); err != nil {
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
					if v := old.GetExtraAttrAsUnsafe("subject", reddo.TypeString); v != "English" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "English", v)
					}
					if v := old.GetExtraAttrAsUnsafe("level", reddo.TypeString); v != "entry" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "entry", v)
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

				if bo, err := dao.Get("id"); err != nil {
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
					if v := bo.GetExtraAttrAsUnsafe("subject", reddo.TypeString); v != "English" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "English", v)
					}
					if v := bo.GetExtraAttrAsUnsafe("level", reddo.TypeString); v != "entry" {
						t.Fatalf("%s failed: expected %#v but received %#v", testName, "entry", v)
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
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// duplicated unique key {dynamodbSingleTablePkPrefix, "email"} or {dynamodbSingleTablePkPrefix, "subject", "level"} should not be allowed.
func TestUniversalDaoDynamodb_SingleTable_SaveExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SingleTable_SaveExistingUnique"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		t.Run(boType, func(t *testing.T) {
			dao1 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
			dao2 := _testDynamodbSingleTableInit(t, testName, testAdc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
				[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

			ubo1 := NewUniversalBo("1", 1357)
			ubo2 := NewUniversalBo("2", 1357)
			ubo1.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
			ubo2.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
			for i, ubo := range []*UniversalBo{ubo1, ubo2} {
				idStr := strconv.Itoa(i + 1)
				ubo.SetExtraAttr("email", idStr+"@mydomain.com")
				ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idStr)

				if boType == "users" {
					ubo.SetDataAttr("testName.first", "Name-"+idStr)
					ubo.SetDataAttr("testName.last", "Nguyen")
					ubo.SetExtraAttr("age", 35+i)
				} else if boType == "products" {
					ubo.SetDataAttr("testName.en", "Product testName (EN) - "+idStr)
					ubo.SetDataAttr("testName.vi", "Product testName (VI) - "+idStr)
					ubo.SetExtraAttr("stock", 35+i)
				}
				for _, dao := range []UniversalDao{dao1, dao2} {
					if _, err := dao.Create(ubo); err != nil {
						t.Fatalf("%s failed: %s", testName, err)
					}
				}
			}

			ubo1.SetExtraAttr("email", "2@mydomain.com")
			if ok, old, err := dao1.Save(ubo1); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot save record", testName)
			} else if old == nil {
				t.Fatalf("%s failed: there should be an existing record", testName)
			}
			if _, _, err := dao2.Save(ubo1); err != godal.ErrGdaoDuplicatedEntry {
				// duplicated email
				t.Fatalf("%s failed: %s", testName, err)
			}

			ubo1.SetExtraAttr("email", "1@mydomain.com")
			ubo1.SetExtraAttr("level", "2")
			if ok, old, err := dao1.Save(ubo1); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot save record", testName)
			} else if old == nil {
				t.Fatalf("%s failed: there should be an existing record", testName)
			}
			if _, _, err := dao2.Save(ubo1); err != godal.ErrGdaoDuplicatedEntry {
				// duplicated {subject:level}
				t.Fatalf("%s failed: %s", testName, err)
			}
		})
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*2 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*2, len(items))
	}
	if items, err := testAdc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*2 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", testName, len(dynamodbSingleTableBoTypes)*2, len(items))
	}
}
