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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/btnguyen2k/consu/checksum"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
)

func TestRowMapperDynamodb_ToRow(t *testing.T) {
	name := "TestRowMapperDynamodb_ToRow"
	rm := buildRowMapperDynamodb("tbl_test", "pk")
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

func TestRowMapperDynamodb_ToBo(t *testing.T) {
	name := "TestRowMapperDynamodb_ToBo"
	rm := buildRowMapperDynamodb("tbl_test", "pk")
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

func TestUniversalDaoDynamodb_pk(t *testing.T) {
	name := "TestUniversalDaoDynamodb_pk"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	dao := NewUniversalDaoDynamodb(adc, "tbl_test", &DynamodbDaoSpec{PkPrefix: "mypk", PkPrefixValue: "mypkvalue"})
	if v := dao.GetPkPrefix(); v != "mypk" {
		t.Fatalf("%s failed: expected %#v but received %#v", name, v, "mypk")
	}
	if v := dao.GetPkPrefixValue(); v != "mypkvalue" {
		t.Fatalf("%s failed: expected %#v but received %#v", name, v, "mypkvalue")
	}
}

func _adbDeleteTableAndWait(adc *prom.AwsDynamodbConnect, tableName string) error {
	if err := adc.DeleteTable(nil, tableName); err != nil {
		return err
	}
	for ok, err := adc.HasTable(nil, tableName); (err == nil && ok) || err != nil; {
		if err != nil {
			fmt.Printf("\tError: %s\n", err)
		}
		fmt.Printf("\tTable %s exists, waiting for deletion...\n", tableName)
		time.Sleep(1 * time.Second)
	}

	uidxTableName := tableName + AwsDynamodbUidxTableSuffix
	if err := adc.DeleteTable(nil, uidxTableName); err != nil {
		return err
	}
	for ok, err := adc.HasTable(nil, uidxTableName); (err == nil && ok) || err != nil; {
		if err != nil {
			fmt.Printf("\tError: %s\n", err)
		}
		fmt.Printf("\tTable %s exists, waiting for deletion...\n", uidxTableName)
		time.Sleep(1 * time.Second)
	}

	return nil
}

func _adbCreateGSIAndWait(adc *prom.AwsDynamodbConnect, tableName, indexName string, rcu, wcu int64, attrDefs, keyAttrs []prom.AwsDynamodbNameAndType) error {
	err := adc.CreateGlobalSecondaryIndex(nil, tableName, indexName, rcu, wcu, attrDefs, keyAttrs)
	if err != nil {
		return err
	}
	for status, err := adc.GetGlobalSecondaryIndexStatus(nil, tableName, indexName); status != "ACTIVE" && err == nil; {
		fmt.Printf("\tGSI [%s] on table [%s] status: %v - %e\n", tableName, indexName, status, err)
		time.Sleep(1 * time.Second)
		status, err = adc.GetGlobalSecondaryIndexStatus(nil, tableName, indexName)
	}
	return nil
}

func _cleanupDynamodb(adc *prom.AwsDynamodbConnect, tableName string) error {
	return _adbDeleteTableAndWait(adc, tableName)
}

func _createAwsDynamodbConnect(t *testing.T, testName string) *prom.AwsDynamodbConnect {
	awsRegion := strings.ReplaceAll(os.Getenv("AWS_REGION"), `"`, "")
	awsAccessKeyId := strings.ReplaceAll(os.Getenv("AWS_ACCESS_KEY_ID"), `"`, "")
	awsSecretAccessKey := strings.ReplaceAll(os.Getenv("AWS_SECRET_ACCESS_KEY"), `"`, "")
	if awsRegion == "" || awsAccessKeyId == "" || awsSecretAccessKey == "" {
		t.Skipf("%s skipped", testName)
		return nil
	}
	cfg := &aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewEnvCredentials(),
	}
	if awsDynamodbEndpoint := strings.ReplaceAll(os.Getenv("AWS_DYNAMODB_ENDPOINT"), `"`, ""); awsDynamodbEndpoint != "" {
		cfg.Endpoint = aws.String(awsDynamodbEndpoint)
		if strings.HasPrefix(awsDynamodbEndpoint, "http://") {
			cfg.DisableSSL = aws.Bool(true)
		}
	}
	adc, err := prom.NewAwsDynamodbConnect(cfg, nil, nil, 10000)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewAwsDynamodbConnect", err)
	}
	return adc
}

var setupTestDynamodb = func(t *testing.T, testName string) {
	testAdc = _createAwsDynamodbConnect(t, testName)
	_adbDeleteTableAndWait(testAdc, testTable)
}

var teardownTestDynamodb = func(t *testing.T, testName string) {
	if testAdc != nil {
		defer func() { testAdc = nil }()
		_adbDeleteTableAndWait(testAdc, testTable+AwsDynamodbUidxTableSuffix)
		_adbDeleteTableAndWait(testAdc, testTable)
	}
}

const (
	awsDynamodbRCU = 2
	awsDynamodbWCU = 1
)

func TestInitDynamodbTables(t *testing.T) {
	testName := "TestInitDynamodbTables"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	tableName := testTable
	_cleanupDynamodb(testAdc, tableName)
	if ok, err := testAdc.HasTable(nil, tableName); err != nil || ok {
		t.Fatalf("%s failed: error [%s] or table [%s] exist", testName, err, tableName)
	}
	if ok, err := testAdc.HasTable(nil, tableName+AwsDynamodbUidxTableSuffix); err != nil || ok {
		t.Fatalf("%s failed: error [%s] or table [%s] exist", testName, err, tableName+AwsDynamodbUidxTableSuffix)
	}
	if err := InitDynamodbTables(testAdc, tableName, nil); err == nil {
		t.Fatalf("%s failed: expected error but received nil", testName)
	}
	if err := InitDynamodbTables(testAdc, tableName, &DynamodbTablesSpec{
		MainTableRcu: awsDynamodbRCU, MainTableWcu: awsDynamodbWCU,
		CreateUidxTable: true, UidxTableRcu: awsDynamodbRCU, UidxTableWcu: awsDynamodbWCU,
	}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if ok, err := testAdc.HasTable(nil, tableName); err != nil || !ok {
		t.Fatalf("%s failed: error [%s] or table [%s] does not exist", testName, err, tableName)
	}
	if ok, err := testAdc.HasTable(nil, tableName+AwsDynamodbUidxTableSuffix); err != nil || !ok {
		t.Fatalf("%s failed: error [%s] or table [%s] does not exist", testName, err, tableName+AwsDynamodbUidxTableSuffix)
	}
}

func _testDynamodbInit(t *testing.T, testName string, adc *prom.AwsDynamodbConnect, tableName string, uidxIndexes [][]string) *UniversalDaoDynamodb {
	if err := InitDynamodbTables(adc, tableName, &DynamodbTablesSpec{
		MainTableRcu: awsDynamodbRCU, MainTableWcu: awsDynamodbWCU,
		CreateUidxTable: uidxIndexes != nil, UidxTableRcu: awsDynamodbRCU, UidxTableWcu: awsDynamodbWCU,
	}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	daoSpec := &DynamodbDaoSpec{UidxAttrs: uidxIndexes}
	return NewUniversalDaoDynamodb(adc, tableName, daoSpec)
}

func TestNewUniversalDaoDynamodb(t *testing.T) {
	testName := "TestNewUniversalDaoDynamodb"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	dao := NewUniversalDaoDynamodb(testAdc, testTable, nil)
	if dao.GetTableName() != testTable {
		t.Fatalf("%s failed: expected table testName %#v but received %#v", testName, testTable, dao.GetTableName())
	}
	if tableNameUidx := testTable + AwsDynamodbUidxTableSuffix; tableNameUidx != dao.GetUidxTableName() {
		t.Fatalf("%s failed: expected table testName %#v but received %#v", testName, tableNameUidx, dao.GetUidxTableName())
	}
	if dao.GetUidxAttrs() != nil {
		t.Fatalf("%s failed: expected Uidx attr %#v but received %#v", testName, nil, dao.GetUidxAttrs())
	}

	uidxAttrs := [][]string{{"email"}, {"subject", "level"}}
	dao = NewUniversalDaoDynamodb(testAdc, testTable, &DynamodbDaoSpec{UidxAttrs: uidxAttrs})
	if dao.GetTableName() != testTable {
		t.Fatalf("%s failed: expected table testName %#v but received %#v", testName, testTable, dao.GetTableName())
	}
	if tableNameUidx := testTable + AwsDynamodbUidxTableSuffix; tableNameUidx != dao.GetUidxTableName() {
		t.Fatalf("%s failed: expected table testName %#v but received %#v", testName, tableNameUidx, dao.GetUidxTableName())
	}
	if !reflect.DeepEqual(uidxAttrs, dao.GetUidxAttrs()) {
		t.Fatalf("%s failed: expected Uidx attr %#v but received %#v", testName, uidxAttrs, dao.GetUidxAttrs())
	}
	dao.SetUidxAttrs(nil)
	if dao.GetUidxAttrs() != nil {
		t.Fatalf("%s failed: expected Uidx attr %#v but received %#v", testName, nil, dao.GetUidxAttrs())
	}
}

func TestUniversalDaoDynamodb_SetGetUidxHashFunctions(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SetGetUidxHashFunctions"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	dao := NewUniversalDaoDynamodb(testAdc, testTable, nil)
	if hfList := dao.GetUidxHashFunctions(); len(hfList) != 2 || hfList[0] == nil || hfList[1] == nil {
		t.Fatalf("%s failed", testName)
	}

	hfListInput := []checksum.HashFunc{checksum.Crc32HashFunc, checksum.Sha512HashFunc, checksum.Md5HashFunc}
	dao.SetUidxHashFunctions(hfListInput)
	if hfList := dao.GetUidxHashFunctions(); len(hfList) != 2 || hfList[0] == nil || hfList[1] == nil {
		t.Fatalf("%s failed", testName)
	}

	dao.SetUidxHashFunctions(nil)
	if hfList := dao.GetUidxHashFunctions(); len(hfList) != 2 || hfList[0] == nil || hfList[1] == nil {
		t.Fatalf("%s failed", testName)
	}
}

const (
	awsDynamodbTableNoUidx = testTable + "_nouidx"
	awsDynamodbTableUidx   = testTable + "_uidx"
)

func TestUniversalDaoDynamodb_Create(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_Create"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

	for _, dao := range []UniversalDao{dao1, dao2} {
		if ok, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", testName)
		}
	}
}

func TestUniversalDaoDynamodb_CreateExistingPK(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_CreateExistingPK"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

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
}

func TestUniversalDaoDynamodb_CreateExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_CreateExistingUnique"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id1", 1357)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

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
}

func TestUniversalDaoDynamodb_CreateGet(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_CreateGet"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

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
}

func TestUniversalDaoDynamodb_CreateDelete(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_CreateDelete"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

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
}

func TestUniversalDaoDynamodb_CreateGetMany(t *testing.T) {
	testName := "TestDynamodb_CreateGetMany"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for _, dao := range []UniversalDao{dao1, dao2} {
		for i := 0; i < 10; i++ {
			ubo := NewUniversalBo(idList[i], uint64(i))
			ubo.SetDataAttr("testName.first", strconv.Itoa(i))
			ubo.SetDataAttr("testName.last", "Nguyen")
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
			ubo.SetExtraAttr("age", 35+i)
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", testName)
			}
		}

		if boList, err := dao.GetAll(nil, nil); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if len(boList) != 10 {
			t.Fatalf("%s failed: expected %#v items but received %#v", testName, 10, len(boList))
		}
	}
}

func TestUniversalDaoDynamodb_CreateGetManyWithFilter(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_CreateGetManyWithFilter"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for _, dao := range []UniversalDao{dao1, dao2} {
		for i := 0; i < 10; i++ {
			ubo := NewUniversalBo(idList[i], uint64(i))
			ubo.SetDataAttr("testName.first", strconv.Itoa(i))
			ubo.SetDataAttr("testName.last", "Nguyen")
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
			ubo.SetExtraAttr("age", 35+i)
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", testName)
			}
		}

		filter := &godal.FilterOptFieldOpValue{FieldName: "age", Operator: godal.FilterOpGreaterOrEqual, Value: 35 + 3}
		if boList, err := dao.GetAll(filter, nil); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if len(boList) != 7 {
			t.Fatalf("%s failed: expected %#v items but received %#v", testName, 7, len(boList))
		}
	}
}

// AWS Dynamodb does not support custom sorting yet
func TestUniversalDaoDynamodb_CreateGetManyWithSorting(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_CreateGetManyWithSorting"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	gsiPlaceholderPartitionField := "__dummy"
	gsiFieldValue := "*"
	gsiFilter := &godal.FilterOptFieldOpValue{FieldName: gsiPlaceholderPartitionField, Operator: godal.FilterOpEqual, Value: gsiFieldValue}
	attrsDef := []prom.AwsDynamodbNameAndType{{Name: gsiPlaceholderPartitionField, Type: prom.AwsAttrTypeString}, {Name: "email", Type: prom.AwsAttrTypeString}}
	keyAttrs := []prom.AwsDynamodbNameAndType{{Name: gsiPlaceholderPartitionField, Type: prom.AwsKeyTypePartition}, {Name: "email", Type: prom.AwsKeyTypeSort}}
	gsiName := "gsi_email"
	sortField := "email"
	if err := testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableNoUidx, gsiName, 1, 1, attrsDef, keyAttrs); err != nil {
		t.Fatalf("%s failed: %s", testName+"/GSI:"+awsDynamodbTableNoUidx, err)
	}
	dao1.MapGsi(gsiName, sortField)
	if err := testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableUidx, gsiName, 1, 1, attrsDef, keyAttrs); err != nil {
		t.Fatalf("%s failed: %s", testName+"/GSI:"+awsDynamodbTableUidx, err)
	}
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
			ubo.SetDataAttr("testName.first", strconv.Itoa(i))
			ubo.SetDataAttr("testName.last", "Nguyen")
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
			ubo.SetExtraAttr("age", 35+i)
			ubo.SetExtraAttr(gsiPlaceholderPartitionField, gsiFieldValue)
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", testName)
			}
		}

		filter := gsiFilter
		sorting := (&godal.SortingField{FieldName: sortField}).ToSortingOpt()
		if boList, err := dao.GetAll(filter, sorting); err != nil {
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
		if boList, err := dao.GetAll(filter, sorting); err != nil {
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
}

// AWS Dynamodb does not support custom sorting yet
func TestUniversalDaoDynamodb_CreateGetManyWithFilterAndSorting(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_CreateGetManyWithFilterAndSorting"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	gsiPlaceholderPartitionField := "__dummy"
	gsiFieldValue := "*"
	gsiFilter := &godal.FilterOptFieldOpValue{FieldName: gsiPlaceholderPartitionField, Operator: godal.FilterOpEqual, Value: gsiFieldValue}
	attrsDef := []prom.AwsDynamodbNameAndType{{Name: gsiPlaceholderPartitionField, Type: prom.AwsAttrTypeString}, {Name: "email", Type: prom.AwsAttrTypeString}}
	keyAttrs := []prom.AwsDynamodbNameAndType{{Name: gsiPlaceholderPartitionField, Type: prom.AwsKeyTypePartition}, {Name: "email", Type: prom.AwsKeyTypeSort}}
	gsiName := "gsi_email"
	sortField := "email"
	if err := testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableNoUidx, gsiName, 1, 1, attrsDef, keyAttrs); err != nil {
		t.Fatalf("%s failed: %s", testName+"/GSI:"+awsDynamodbTableNoUidx, err)
	}
	dao1.MapGsi(gsiName, sortField)
	if err := testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableUidx, gsiName, 1, 1, attrsDef, keyAttrs); err != nil {
		t.Fatalf("%s failed: %s", testName+"/GSI:"+awsDynamodbTableUidx, err)
	}
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
			ubo.SetDataAttr("testName.first", strconv.Itoa(i))
			ubo.SetDataAttr("testName.last", "Nguyen")
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
			ubo.SetExtraAttr("age", 35+i)
			ubo.SetExtraAttr(gsiPlaceholderPartitionField, gsiFieldValue)
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", testName)
			}
		}

		sorting := (&godal.SortingField{FieldName: sortField}).ToSortingOpt()
		filter := (&godal.FilterOptAnd{}).Add(gsiFilter).
			Add(&godal.FilterOptFieldOpValue{FieldName: "email", Operator: godal.FilterOpLess, Value: "3@mydomain.com"})
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
		filter = (&godal.FilterOptAnd{}).Add(gsiFilter).
			Add(&godal.FilterOptFieldOpValue{FieldName: "email", Operator: godal.FilterOpLess, Value: "3@mydomain.com"})
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
}

// AWS Dynamodb does not support custom sorting yet
func TestUniversalDaoDynamodb_CreateGetManyWithSortingAndPaging(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_CreateGetManyWithSortingAndPaging"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	gsiPlaceholderPartitionField := "__dummy"
	gsiFieldValue := "*"
	gsiFilter := &godal.FilterOptFieldOpValue{FieldName: gsiPlaceholderPartitionField, Operator: godal.FilterOpEqual, Value: gsiFieldValue}
	attrsDef := []prom.AwsDynamodbNameAndType{{Name: gsiPlaceholderPartitionField, Type: prom.AwsAttrTypeString}, {Name: "email", Type: prom.AwsAttrTypeString}}
	keyAttrs := []prom.AwsDynamodbNameAndType{{Name: gsiPlaceholderPartitionField, Type: prom.AwsKeyTypePartition}, {Name: "email", Type: prom.AwsKeyTypeSort}}
	gsiName := "gsi_email"
	sortField := "email"
	if err := testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableNoUidx, gsiName, 1, 1, attrsDef, keyAttrs); err != nil {
		t.Fatalf("%s failed: %s", testName+"/GSI:"+awsDynamodbTableNoUidx, err)
	}
	dao1.MapGsi(gsiName, sortField)
	if err := testAdc.CreateGlobalSecondaryIndex(nil, awsDynamodbTableUidx, gsiName, 1, 1, attrsDef, keyAttrs); err != nil {
		t.Fatalf("%s failed: %s", testName+"/GSI:"+awsDynamodbTableUidx, err)
	}
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
			ubo.SetDataAttr("testName.first", strconv.Itoa(i))
			ubo.SetDataAttr("testName.last", "Nguyen")
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
			ubo.SetExtraAttr("age", 35+i)
			ubo.SetExtraAttr(gsiPlaceholderPartitionField, gsiFieldValue)
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", testName)
			}
		}

		fromOffset := 3
		numRows := 4
		filter := gsiFilter

		sorting := (&godal.SortingField{FieldName: sortField}).ToSortingOpt()
		if boList, err := dao.GetN(fromOffset, numRows, filter, sorting); err != nil {
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
		if boList, err := dao.GetN(fromOffset, numRows, filter, sorting); err != nil {
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
}

func TestUniversalDaoDynamodb_CreateGetManyWithPaging(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_CreateGetManyWithPaging"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for _, dao := range []UniversalDao{dao1, dao2} {
		for i := 0; i < 10; i++ {
			ubo := NewUniversalBo(idList[i], uint64(i))
			ubo.SetDataAttr("testName.first", strconv.Itoa(i))
			ubo.SetDataAttr("testName.last", "Nguyen")
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
			ubo.SetExtraAttr("age", 35+i)
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", testName)
			}
		}

		fromOffset := 3
		numRows := 4
		if boList, err := dao.GetN(fromOffset, numRows, nil, nil); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if len(boList) != numRows {
			t.Fatalf("%s failed: expected %#v items but received %#v", testName, numRows, len(boList))
		} else if len(boList) != numRows {
			t.Fatalf("%s failed: expected %#v items but received %#v", testName, numRows, len(boList))
		}
	}
}

func TestUniversalDaoDynamodb_Update(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_Update"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

	for _, dao := range []UniversalDao{dao1, dao2} {
		if _, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}
	}

	ubo.SetDataAttr("testName.first", "Thanh2")
	ubo.SetDataAttr("testName.last", "Nguyen2")
	ubo.SetExtraAttr("email", "thanh@mydomain.com")
	ubo.SetExtraAttr("subject", "Maths").SetExtraAttr("level", "advanced")
	ubo.SetExtraAttr("age", 37)

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
			if v := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh2" {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh2", v)
			}
			if v := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen2" {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen2", v)
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
			if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(37), v)
			}
		}
	}
}

func TestUniversalDaoDynamodb_UpdateNotExist(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_UpdateNotExist"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

	for _, dao := range []UniversalDao{dao1, dao2} {
		if ok, err := dao.Update(ubo); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if ok {
			t.Fatalf("%s failed: record should not be updated", testName)
		}
	}
}

func TestUniversalDaoDynamodb_UpdateDuplicated(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_UpdateDuplicated"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo1 := NewUniversalBo("1", 1357)
	ubo2 := NewUniversalBo("2", 1357)
	for i, ubo := range []*UniversalBo{ubo1, ubo2} {
		idStr := strconv.Itoa(i + 1)
		ubo.SetDataAttr("testName.first", "Name-"+idStr)
		ubo.SetDataAttr("testName.last", "Nguyen")
		ubo.SetExtraAttr("email", idStr+"@mydomain.com")
		ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idStr)
		ubo.SetExtraAttr("age", 35+i)
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
}

func TestUniversalDaoDynamodb_SaveNew(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SaveNew"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

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
			if v := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh" {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh", v)
			}
			if v := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen" {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen", v)
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
			if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(35), v)
			}
		}
	}
}

func TestUniversalDaoDynamodb_SaveExisting(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SaveExisting"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("testName.first", "Thanh")
	ubo.SetDataAttr("testName.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

	for _, dao := range []UniversalDao{dao1, dao2} {
		if _, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}
	}

	ubo.SetDataAttr("testName.first", "Thanh2")
	ubo.SetDataAttr("testName.last", "Nguyen2")
	ubo.SetExtraAttr("age", 37)
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
			if v := old.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh" {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh", v)
			}
			if v := old.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen" {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen", v)
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
			if v := old.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(35), v)
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
			if v := bo.GetDataAttrAsUnsafe("testName.first", reddo.TypeString); v != "Thanh2" {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, "Thanh2", v)
			}
			if v := bo.GetDataAttrAsUnsafe("testName.last", reddo.TypeString); v != "Nguyen2" {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, "Nguyen2", v)
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
			if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
				t.Fatalf("%s failed: expected %#v but received %#v", testName, int64(37), v)
			}
		}
	}
}

func TestUniversalDaoDynamodb_SaveExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_SaveExistingUnique"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(testAdc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, testName, testAdc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo1 := NewUniversalBo("1", 1357)
	ubo2 := NewUniversalBo("2", 1357)
	for i, ubo := range []*UniversalBo{ubo1, ubo2} {
		idStr := strconv.Itoa(i + 1)
		ubo.SetDataAttr("testName.first", "Name-"+idStr)
		ubo.SetDataAttr("testName.last", "Nguyen")
		ubo.SetExtraAttr("email", idStr+"@mydomain.com")
		ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idStr)
		ubo.SetExtraAttr("age", 35+i)
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
}

func TestUniversalDaoDynamodb_CreateUpdateGet_Checksum(t *testing.T) {
	testName := "TestUniversalDaoDynamodb_CreateUpdateGet_Checksum"
	teardownTest := setupTest(t, testName, setupTestDynamodb, teardownTestDynamodb)
	defer teardownTest(t)

	_cleanupDynamodb(testAdc, testTable)
	dao := _testDynamodbInit(t, testName, testAdc, testTable, nil)

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
	if ok, err := dao.Create(&(user0.sync().UniversalBo)); err != nil {
		t.Fatalf("%s failed: %s", testName+"/Create", err)
	} else if !ok {
		t.Fatalf("%s failed: cannot create record", testName)
	}
	if bo, err := dao.Get(_id); err != nil {
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
	if ok, err := dao.Update(&(user0.sync().UniversalBo)); err != nil {
		t.Fatalf("%s failed: %s", testName+"/Update", err)
	} else if !ok {
		t.Fatalf("%s failed: cannot update record", testName)
	}
	if bo, err := dao.Get(_id); err != nil {
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
