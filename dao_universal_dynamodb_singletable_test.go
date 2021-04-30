package henge

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
)

const (
	dynamodbSingleTablePkPrefix = "pk"
)

var (
	dynamodbSingleTableBoTypes = []string{"users", "products", "none"}
)

func _testDynamodbSingleTableInit(t *testing.T, testName string, adc *prom.AwsDynamodbConnect, tableName, pkPrefix, pkPrefixValue string, uidxIndexes [][]string) UniversalDao {
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
func TestDynamodbSingleTable_Create(t *testing.T) {
	name := "TestDynamodbSingleTable_Create"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
			[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

		for _, dao := range []UniversalDao{dao1, dao2} {
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", name)
			}
		}
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// duplicated PK {dynamodbSingleTablePkPrefix, FieldId} should not be allowed.
func TestDynamodbSingleTable_CreateExistingPK(t *testing.T) {
	name := "TestDynamodbSingleTable_CreateExistingPK"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
			[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

		for _, dao := range []UniversalDao{dao1, dao2} {
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", name)
			}
		}

		ubo.SetExtraAttr("email", "myname2@mydomain.com")
		for _, dao := range []UniversalDao{dao1, dao2} {
			if ok, err := dao.Create(ubo); err != godal.ErrGdaoDuplicatedEntry {
				t.Fatalf("%s failed: %s", name, err)
			} else if ok {
				t.Fatalf("%s failed: record should not be created twice", name)
			}
		}
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// duplicated unique key {dynamodbSingleTablePkPrefix, "email"} or {dynamodbSingleTablePkPrefix, "subject", "level"} should not be allowed.
func TestDynamodbSingleTable_CreateExistingUnique(t *testing.T) {
	name := "TestDynamodbSingleTable_CreateExistingUnique"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
			[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

		for _, dao := range []UniversalDao{dao1, dao2} {
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", name)
			}
		}

		ubo.SetId("id2").SetExtraAttr("subject", "English2").SetExtraAttr("level", "entry2")
		if ok, err := dao1.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", name)
		}
		if ok, err := dao2.Create(ubo); err != godal.ErrGdaoDuplicatedEntry {
			// duplicated "email"
			t.Fatalf("%s failed: %s", name, err)
		} else if ok {
			t.Fatalf("%s failed: record should not be created twice", name)
		}

		ubo.SetId("id3").SetExtraAttr("email", "another@mydomain.com").
			SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
		if ok, err := dao1.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", name)
		}
		if ok, err := dao2.Create(ubo); err != godal.ErrGdaoDuplicatedEntry {
			// duplicated {"subject","level"}
			t.Fatalf("%s failed: %s", name, err)
		} else if ok {
			t.Fatalf("%s failed: record should not be created twice", name)
		}
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*3 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes)*3, len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestDynamodbSingleTable_CreateGet(t *testing.T) {
	name := "TestDynamodbSingleTable_CreateGet"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
			[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

		for _, dao := range []UniversalDao{dao1, dao2} {
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
				if v := bo.GetExtraAttrAsUnsafe("subject", reddo.TypeString); v != "English" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "English", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("level", reddo.TypeString); v != "entry" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "entry", v)
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
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestDynamodbSingleTable_CreateDelete(t *testing.T) {
	name := "TestDynamodbSingleTable_CreateDelete"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
			[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

		for _, dao := range []UniversalDao{dao1, dao2} {
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
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != 0 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, 0, len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != 0 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, 0, len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestDynamodbSingleTable_CreateGetMany(t *testing.T) {
	name := "TestDynamodbSingleTable_CreateGetMany"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
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
					ubo.SetDataAttr("name.first", strconv.Itoa(i))
					ubo.SetDataAttr("name.last", "Nguyen")
					ubo.SetExtraAttr("age", 35+i)
				} else if boType == "products" {
					ubo.SetDataAttr("name.en", strconv.Itoa(i)+" (EN)")
					ubo.SetDataAttr("name.vi", strconv.Itoa(i)+" (VI)")
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
				t.Fatalf("%s failed: expected %#v items but received %#v", name+"/"+boType, 10, len(boList))
			}
		}
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestDynamodbSingleTable_CreateGetManyWithFilter(t *testing.T) {
	name := "TestDynamodbSingleTable_CreateGetManyWithFilter"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
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
					ubo.SetDataAttr("name.first", strconv.Itoa(i))
					ubo.SetDataAttr("name.last", "Nguyen")
					ubo.SetExtraAttr("age", 35+i)
				} else if boType == "products" {
					ubo.SetDataAttr("name.en", strconv.Itoa(i)+" (EN)")
					ubo.SetDataAttr("name.vi", strconv.Itoa(i)+" (VI)")
					ubo.SetExtraAttr("stock", 35+i)
				}
				if ok, err := dao.Create(ubo); err != nil {
					t.Fatalf("%s failed: %s", name, err)
				} else if !ok {
					t.Fatalf("%s failed: cannot create record", name)
				}
			}

			filter := expression.Name("email").GreaterThanEqual(expression.Value("3@mydomain.com"))
			if boType == "users" {
				filter = expression.Name("age").GreaterThanEqual(expression.Value(35 + 3))
			} else if boType == "products" {
				filter = expression.Name("stock").GreaterThanEqual(expression.Value(35 + 3))
			}
			if boList, err := dao.GetAll(filter, nil); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if len(boList) != 7 {
				t.Fatalf("%s failed: expected %#v items but received %#v", name, 7, len(boList))
			}
		}
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
}

// AWS Dynamodb does not support custom sorting yet
func TestDynamodbSingleTable_CreateGetManyWithSorting(t *testing.T) {
	// name := "TestDynamodbSingleTable_CreateGetManyWithSorting"
}

// AWS Dynamodb does not support custom sorting yet
func TestDynamodbSingleTable_CreateGetManyWithFilterAndSorting(t *testing.T) {
	// name := "TestDynamodbSingleTable_CreateGetManyWithFilterAndSorting"
}

// AWS Dynamodb does not support custom sorting yet
func TestDynamodbSingleTable_CreateGetManyWithSortingAndPaging(t *testing.T) {
	// 	name := "TestDynamodbSingleTable_CreateGetManyWithSortingAndPaging"
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestDynamodbSingleTable_CreateGetManyWithPaging(t *testing.T) {
	name := "TestDynamodbSingleTable_CreateGetManyWithPaging"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
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
					ubo.SetDataAttr("name.first", strconv.Itoa(i))
					ubo.SetDataAttr("name.last", "Nguyen")
					ubo.SetExtraAttr("age", 35+i)
				} else if boType == "products" {
					ubo.SetDataAttr("name.en", strconv.Itoa(i)+" (EN)")
					ubo.SetDataAttr("name.vi", strconv.Itoa(i)+" (VI)")
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
			filter := expression.Name("email").GreaterThanEqual(expression.Value("3@mydomain.com"))
			if boType == "users" {
				filter = expression.Name("age").GreaterThanEqual(expression.Value(35 + 3))
			} else if boType == "products" {
				filter = expression.Name("stock").GreaterThanEqual(expression.Value(35 + 3))
			}
			if boList, err := dao.GetN(fromOffset, numRows, filter, nil); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if len(boList) != numRows {
				t.Fatalf("%s failed: expected %#v items but received %#v", name, numRows, len(boList))
			} else {
				for _, bo := range boList {
					if boType == "users" && bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt).(int64) < 35+3 {
						t.Fatalf("%s failed: expected value >= %#v but received %#v", name+"/"+boType, 35+3, bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt))
					} else if boType == "products" && bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt).(int64) < 35+3 {
						t.Fatalf("%s failed: expected value >= %#v but received %#v", name+"/"+boType, 35+3, bo.GetExtraAttrAsUnsafe("stock", reddo.TypeInt))
					} else if bo.GetExtraAttrAsUnsafe("email", reddo.TypeString).(string) < "3@mydomain.com" {
						t.Fatalf("%s failed: expected value >= %#v but received %#v", name+"/"+boType, "3@mydomain.com", bo.GetExtraAttrAsUnsafe("email", reddo.TypeString))
					}
				}
			}
		}
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*10 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes)*10, len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestDynamodbSingleTable_Update(t *testing.T) {
	name := "TestDynamodbSingleTable_Update"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
			[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

		for _, dao := range []UniversalDao{dao1, dao2} {
			if _, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			}
		}

		ubo.SetExtraAttr("email", "thanh@mydomain.com")
		ubo.SetExtraAttr("subject", "Maths").SetExtraAttr("level", "advanced")
		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh2")
			ubo.SetDataAttr("name.last", "Nguyen2")
			ubo.SetExtraAttr("age", 37)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN2)")
			ubo.SetDataAttr("name.vi", "Product name (VI2)")
			ubo.SetExtraAttr("stock", 37)
		}

		for _, dao := range []UniversalDao{dao1, dao2} {
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
				if v := bo.GetExtraAttrAsUnsafe("subject", reddo.TypeString); v != "Maths" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "Maths", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("level", reddo.TypeString); v != "advanced" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "advanced", v)
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
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestDynamodbSingleTable_UpdateNotExist(t *testing.T) {
	name := "TestDynamodbSingleTable_UpdateNotExist"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
			[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

		for _, dao := range []UniversalDao{dao1, dao2} {
			if ok, err := dao.Update(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if ok {
				t.Fatalf("%s failed: record should not be updated", name)
			}
		}
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != 0 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, 0, len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != 0 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, 0, len(items))
	}
}

// duplicated unique key {dynamodbSingleTablePkPrefix, "email"} or {dynamodbSingleTablePkPrefix, "subject", "level"} should not be allowed.
func TestDynamodbSingleTable_UpdateDuplicated(t *testing.T) {
	name := "TestDynamodbSingleTable_UpdateDuplicated"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
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
				ubo.SetDataAttr("name.first", "Name-"+idStr)
				ubo.SetDataAttr("name.last", "Nguyen")
				ubo.SetExtraAttr("age", 35+i)
			} else if boType == "products" {
				ubo.SetDataAttr("name.en", "Product name (EN) - "+idStr)
				ubo.SetDataAttr("name.vi", "Product name (VI) - "+idStr)
				ubo.SetExtraAttr("stock", 35+i)
			}
			for _, dao := range []UniversalDao{dao1, dao2} {
				if _, err := dao.Create(ubo); err != nil {
					t.Fatalf("%s failed: %s", name, err)
				}
			}
		}

		ubo1.SetExtraAttr("email", "2@mydomain.com")
		if _, err := dao1.Update(ubo1); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		if _, err := dao2.Update(ubo1); err != godal.ErrGdaoDuplicatedEntry {
			// duplicated email
			t.Fatalf("%s failed: %s", name, err)
		}

		ubo1.SetExtraAttr("email", "1@mydomain.com")
		ubo1.SetExtraAttr("level", "2")
		if _, err := dao1.Update(ubo1); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		if _, err := dao2.Update(ubo1); err != godal.ErrGdaoDuplicatedEntry {
			// duplicated {subject:level}
			t.Fatalf("%s failed: %s", name, err)
		}
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*2 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes)*2, len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*2 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes)*2, len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestDynamodbSingleTable_SaveNew(t *testing.T) {
	name := "TestDynamodbSingleTable_SaveNew"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
			[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

		for _, dao := range []UniversalDao{dao1, dao2} {
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
				if v := bo.GetExtraAttrAsUnsafe("subject", reddo.TypeString); v != "English" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "English", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("level", reddo.TypeString); v != "entry" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "entry", v)
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
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// one single DynamoDB table should be able to store multiple types of BO.
func TestDynamodbSingleTable_SaveExisting(t *testing.T) {
	name := "TestDynamodbSingleTable_SaveExisting"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
			[][]string{{dynamodbSingleTablePkPrefix, "email"}, {dynamodbSingleTablePkPrefix, "subject", "level"}})

		ubo := NewUniversalBo("id", 1357)
		ubo.SetExtraAttr(dynamodbSingleTablePkPrefix, boType)
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

		for _, dao := range []UniversalDao{dao1, dao2} {
			if _, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			}
		}

		if boType == "users" {
			ubo.SetDataAttr("name.first", "Thanh2")
			ubo.SetDataAttr("name.last", "Nguyen2")
			ubo.SetExtraAttr("age", 37)
		} else if boType == "products" {
			ubo.SetDataAttr("name.en", "Product name (EN2)")
			ubo.SetDataAttr("name.vi", "Product name (VI2)")
			ubo.SetExtraAttr("stock", 37)
		}
		for _, dao := range []UniversalDao{dao1, dao2} {
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
				if v := old.GetExtraAttrAsUnsafe("subject", reddo.TypeString); v != "English" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "English", v)
				}
				if v := old.GetExtraAttrAsUnsafe("level", reddo.TypeString); v != "entry" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "entry", v)
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
				if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "myname@mydomain.com", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("subject", reddo.TypeString); v != "English" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "English", v)
				}
				if v := bo.GetExtraAttrAsUnsafe("level", reddo.TypeString); v != "entry" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "entry", v)
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
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes) {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes), len(items))
	}
}

// duplicated unique key {dynamodbSingleTablePkPrefix, "email"} or {dynamodbSingleTablePkPrefix, "subject", "level"} should not be allowed.
func TestDynamodbSingleTable_SaveExistingUnique(t *testing.T) {
	name := "TestDynamodbSingleTable_SaveExistingUnique"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	for _, boType := range dynamodbSingleTableBoTypes {
		dao1 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableNoUidx, dynamodbSingleTablePkPrefix, boType, nil)
		dao2 := _testDynamodbSingleTableInit(t, name, adc, awsDynamodbTableUidx, dynamodbSingleTablePkPrefix, boType,
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
				ubo.SetDataAttr("name.first", "Name-"+idStr)
				ubo.SetDataAttr("name.last", "Nguyen")
				ubo.SetExtraAttr("age", 35+i)
			} else if boType == "products" {
				ubo.SetDataAttr("name.en", "Product name (EN) - "+idStr)
				ubo.SetDataAttr("name.vi", "Product name (VI) - "+idStr)
				ubo.SetExtraAttr("stock", 35+i)
			}
			for _, dao := range []UniversalDao{dao1, dao2} {
				if _, err := dao.Create(ubo); err != nil {
					t.Fatalf("%s failed: %s", name, err)
				}
			}
		}

		ubo1.SetExtraAttr("email", "2@mydomain.com")
		if ok, old, err := dao1.Save(ubo1); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot save record", name)
		} else if old == nil {
			t.Fatalf("%s failed: there should be an existing record", name)
		}
		if _, _, err := dao2.Save(ubo1); err != godal.ErrGdaoDuplicatedEntry {
			// duplicated email
			t.Fatalf("%s failed: %s", name, err)
		}

		ubo1.SetExtraAttr("email", "1@mydomain.com")
		ubo1.SetExtraAttr("level", "2")
		if ok, old, err := dao1.Save(ubo1); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot save record", name)
		} else if old == nil {
			t.Fatalf("%s failed: there should be an existing record", name)
		}
		if _, _, err := dao2.Save(ubo1); err != godal.ErrGdaoDuplicatedEntry {
			// duplicated {subject:level}
			t.Fatalf("%s failed: %s", name, err)
		}
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableNoUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*2 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes)*2, len(items))
	}
	if items, err := adc.ScanItems(nil, awsDynamodbTableUidx, nil, ""); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if len(items) != len(dynamodbSingleTableBoTypes)*2 {
		t.Fatalf("%s failed: expected table to have %#v rows but received %#v", name, len(dynamodbSingleTableBoTypes)*2, len(items))
	}
}
