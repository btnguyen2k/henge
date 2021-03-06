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
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
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
	dDao, ok := dao.(*UniversalDaoDynamodb)
	if !ok || dDao == nil {
		t.Fatalf("%s failed: nil", name)
	}
	if v := dDao.GetPkPrefix(); v != "mypk" {
		t.Fatalf("%s failed: expected %#v but received %#v", name, v, "mypk")
	}
	if v := dDao.GetPkPrefixValue(); v != "mypkvalue" {
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

func TestNewDynamodbConnection(t *testing.T) {
	name := "TestNewDynamodbConnection"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
}

const (
	awsDynamodbRCU = 2
	awsDynamodbWCU = 1
)

func TestInitDynamodbTables(t *testing.T) {
	name := "TestInitDynamodbTables"
	tableName := "table_temp"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, tableName)
	if ok, err := adc.HasTable(nil, tableName); err != nil || ok {
		t.Fatalf("%s failed: error [%s] or table [%s] exist", name, err, tableName)
	}
	if ok, err := adc.HasTable(nil, tableName+AwsDynamodbUidxTableSuffix); err != nil || ok {
		t.Fatalf("%s failed: error [%s] or table [%s] exist", name, err, tableName+AwsDynamodbUidxTableSuffix)
	}
	if err := InitDynamodbTables(adc, tableName, nil); err == nil {
		t.Fatalf("%s failed: expected error but received nil", name)
	}
	if err := InitDynamodbTables(adc, tableName, &DynamodbTablesSpec{
		MainTableRcu: awsDynamodbRCU, MainTableWcu: awsDynamodbWCU,
		CreateUidxTable: true, UidxTableRcu: awsDynamodbRCU, UidxTableWcu: awsDynamodbWCU,
	}); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	if ok, err := adc.HasTable(nil, tableName); err != nil || !ok {
		t.Fatalf("%s failed: error [%s] or table [%s] does not exist", name, err, tableName)
	}
	if ok, err := adc.HasTable(nil, tableName+AwsDynamodbUidxTableSuffix); err != nil || !ok {
		t.Fatalf("%s failed: error [%s] or table [%s] does not exist", name, err, tableName+AwsDynamodbUidxTableSuffix)
	}
}

func _testDynamodbInit(t *testing.T, testName string, adc *prom.AwsDynamodbConnect, tableName string, uidxIndexes [][]string) UniversalDao {
	if err := InitDynamodbTables(adc, tableName, &DynamodbTablesSpec{
		MainTableRcu: awsDynamodbRCU, MainTableWcu: awsDynamodbWCU,
		CreateUidxTable: true, UidxTableRcu: awsDynamodbRCU, UidxTableWcu: awsDynamodbWCU,
	}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	daoSpec := &DynamodbDaoSpec{UidxAttrs: uidxIndexes}
	return NewUniversalDaoDynamodb(adc, tableName, daoSpec)
}

func TestNewUniversalDaoDynamodb(t *testing.T) {
	name := "TestNewUniversalDaoDynamodb"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	tableName := "tbl_test"
	dao := NewUniversalDaoDynamodb(adc, tableName, nil).(*UniversalDaoDynamodb)
	if dao.GetTableName() != tableName {
		t.Fatalf("%s failed: expected table name %#v but received %#v", name, tableName, dao.GetTableName())
	}
	if tableNameUidx := tableName + AwsDynamodbUidxTableSuffix; tableNameUidx != dao.GetUidxTableName() {
		t.Fatalf("%s failed: expected table name %#v but received %#v", name, tableNameUidx, dao.GetUidxTableName())
	}
	if dao.GetUidxAttrs() != nil {
		t.Fatalf("%s failed: expected Uidx attr %#v but received %#v", name, nil, dao.GetUidxAttrs())
	}

	uidxAttrs := [][]string{{"email"}, {"subject", "level"}}
	dao = NewUniversalDaoDynamodb(adc, tableName, &DynamodbDaoSpec{UidxAttrs: uidxAttrs}).(*UniversalDaoDynamodb)
	if dao.GetTableName() != tableName {
		t.Fatalf("%s failed: expected table name %#v but received %#v", name, tableName, dao.GetTableName())
	}
	if tableNameUidx := tableName + AwsDynamodbUidxTableSuffix; tableNameUidx != dao.GetUidxTableName() {
		t.Fatalf("%s failed: expected table name %#v but received %#v", name, tableNameUidx, dao.GetUidxTableName())
	}
	if !reflect.DeepEqual(uidxAttrs, dao.GetUidxAttrs()) {
		t.Fatalf("%s failed: expected Uidx attr %#v but received %#v", name, uidxAttrs, dao.GetUidxAttrs())
	}
	dao.SetUidxAttrs(nil)
	if dao.GetUidxAttrs() != nil {
		t.Fatalf("%s failed: expected Uidx attr %#v but received %#v", name, nil, dao.GetUidxAttrs())
	}
}

func TestUniversalDaoDynamodb_SetGetUidxHashFunctions(t *testing.T) {
	name := "TestUniversalDaoDynamodb_SetGetUidxHashFunctions"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	tableName := "tbl_test"
	dao := NewUniversalDaoDynamodb(adc, tableName, nil).(*UniversalDaoDynamodb)
	if hfList := dao.GetUidxHashFunctions(); len(hfList) != 2 || hfList[0] == nil || hfList[1] == nil {
		t.Fatalf("%s failed", name)
	}

	hfListInput := []checksum.HashFunc{checksum.Crc32HashFunc, checksum.Sha512HashFunc, checksum.Md5HashFunc}
	dao.SetUidxHashFunctions(hfListInput)
	if hfList := dao.GetUidxHashFunctions(); len(hfList) != 2 || hfList[0] == nil || hfList[1] == nil {
		t.Fatalf("%s failed", name)
	}

	dao.SetUidxHashFunctions(nil)
	if hfList := dao.GetUidxHashFunctions(); len(hfList) != 2 || hfList[0] == nil || hfList[1] == nil {
		t.Fatalf("%s failed", name)
	}
}

const (
	awsDynamodbTableNoUidx = "tbl_nouidx"
	awsDynamodbTableUidx   = "tbl_uidx"
)

func TestDynamodb_Create(t *testing.T) {
	name := "TestDynamodb_Create"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

	for _, dao := range []UniversalDao{dao1, dao2} {
		if ok, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !ok {
			t.Fatalf("%s failed: cannot create record", name)
		}
	}
}

func TestDynamodb_CreateExistingPK(t *testing.T) {
	name := "TestDynamodb_CreateExistingPK"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

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

func TestDynamodb_CreateExistingUnique(t *testing.T) {
	name := "TestDynamodb_CreateExistingUnique"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id1", 1357)
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

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

func TestDynamodb_CreateGet(t *testing.T) {
	name := "TestDynamodb_CreateGet"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

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
}

func TestDynamodb_CreateDelete(t *testing.T) {
	name := "TestDynamodb_CreateDelete"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

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

func TestDynamodb_CreateGetMany(t *testing.T) {
	name := "TestDynamodb_CreateGetMany"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for _, dao := range []UniversalDao{dao1, dao2} {
		for i := 0; i < 10; i++ {
			ubo := NewUniversalBo(idList[i], uint64(i))
			ubo.SetDataAttr("name.first", strconv.Itoa(i))
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
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
}

func TestDynamodb_CreateGetManyWithFilter(t *testing.T) {
	name := "TestDynamodb_CreateGetManyWithFilter"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for _, dao := range []UniversalDao{dao1, dao2} {
		for i := 0; i < 10; i++ {
			ubo := NewUniversalBo(idList[i], uint64(i))
			ubo.SetDataAttr("name.first", strconv.Itoa(i))
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
			ubo.SetExtraAttr("age", 35+i)
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", name)
			}
		}

		filter := expression.Name("age").GreaterThanEqual(expression.Value(35 + 3))
		if boList, err := dao.GetAll(filter, nil); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if len(boList) != 7 {
			t.Fatalf("%s failed: expected %#v items but received %#v", name, 7, len(boList))
		}
	}
}

// AWS Dynamodb does not support custom sorting yet
func TestDynamodb_CreateGetManyWithSorting(t *testing.T) {
	// name := "TestDynamodb_CreateGetManyWithSorting"
}

// AWS Dynamodb does not support custom sorting yet
func TestDynamodb_CreateGetManyWithFilterAndSorting(t *testing.T) {
	// name := "TestDynamodb_CreateGetManyWithFilterAndSorting"
}

// AWS Dynamodb does not support custom sorting yet
func TestDynamodb_CreateGetManyWithSortingAndPaging(t *testing.T) {
	// 	name := "TestDynamodb_CreateGetManyWithSortingAndPaging"
}

func TestDynamodb_CreateGetManyWithPaging(t *testing.T) {
	name := "TestDynamodb_CreateGetManyWithPaging"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	idList := make([]string, 0)
	for i := 0; i < 10; i++ {
		idList = append(idList, strconv.Itoa(i))
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(idList), func(i, j int) { idList[i], idList[j] = idList[j], idList[i] })
	for _, dao := range []UniversalDao{dao1, dao2} {
		for i := 0; i < 10; i++ {
			ubo := NewUniversalBo(idList[i], uint64(i))
			ubo.SetDataAttr("name.first", strconv.Itoa(i))
			ubo.SetDataAttr("name.last", "Nguyen")
			ubo.SetExtraAttr("email", idList[i]+"@mydomain.com")
			ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idList[i])
			ubo.SetExtraAttr("age", 35+i)
			if ok, err := dao.Create(ubo); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if !ok {
				t.Fatalf("%s failed: cannot create record", name)
			}
		}

		fromOffset := 3
		numRows := 4
		if boList, err := dao.GetN(fromOffset, numRows, nil, nil); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if len(boList) != numRows {
			t.Fatalf("%s failed: expected %#v items but received %#v", name, numRows, len(boList))
		} else if len(boList) != numRows {
			t.Fatalf("%s failed: expected %#v items but received %#v", name, numRows, len(boList))
		}
	}
}

func TestDynamodb_Update(t *testing.T) {
	name := "TestDynamodb_Update"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

	for _, dao := range []UniversalDao{dao1, dao2} {
		if _, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}

	ubo.SetDataAttr("name.first", "Thanh2")
	ubo.SetDataAttr("name.last", "Nguyen2")
	ubo.SetExtraAttr("email", "thanh@mydomain.com")
	ubo.SetExtraAttr("subject", "Maths").SetExtraAttr("level", "advanced")
	ubo.SetExtraAttr("age", 37)

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
			if v := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh2" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh2", v)
			}
			if v := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen2" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen2", v)
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
			if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
				t.Fatalf("%s failed: expected %#v but received %#v", name, int64(37), v)
			}
		}
	}
}

func TestDynamodb_UpdateNotExist(t *testing.T) {
	name := "TestDynamodb_UpdateNotExist"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

	for _, dao := range []UniversalDao{dao1, dao2} {
		if ok, err := dao.Update(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if ok {
			t.Fatalf("%s failed: record should not be updated", name)
		}
	}
}

func TestDynamodb_UpdateDuplicated(t *testing.T) {
	name := "TestDynamodb_UpdateDuplicated"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo1 := NewUniversalBo("1", 1357)
	ubo2 := NewUniversalBo("2", 1357)
	for i, ubo := range []*UniversalBo{ubo1, ubo2} {
		idStr := strconv.Itoa(i + 1)
		ubo.SetDataAttr("name.first", "Name-"+idStr)
		ubo.SetDataAttr("name.last", "Nguyen")
		ubo.SetExtraAttr("email", idStr+"@mydomain.com")
		ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idStr)
		ubo.SetExtraAttr("age", 35+i)
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

func TestDynamodb_SaveNew(t *testing.T) {
	name := "TestDynamodb_SaveNew"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

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
			if v := bo.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh", v)
			}
			if v := bo.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen", v)
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
			if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(35) {
				t.Fatalf("%s failed: expected %#v but received %#v", name, int64(35), v)
			}
		}
	}
}

func TestDynamodb_SaveExisting(t *testing.T) {
	name := "TestDynamodb_SaveExisting"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo := NewUniversalBo("id", 1357)
	ubo.SetDataAttr("name.first", "Thanh")
	ubo.SetDataAttr("name.last", "Nguyen")
	ubo.SetExtraAttr("email", "myname@mydomain.com")
	ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", "entry")
	ubo.SetExtraAttr("age", 35)

	for _, dao := range []UniversalDao{dao1, dao2} {
		if _, err := dao.Create(ubo); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}

	ubo.SetDataAttr("name.first", "Thanh2")
	ubo.SetDataAttr("name.last", "Nguyen2")
	ubo.SetExtraAttr("age", 37)
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
			if v := old.GetDataAttrAsUnsafe("name.first", reddo.TypeString); v != "Thanh" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "Thanh", v)
			}
			if v := old.GetDataAttrAsUnsafe("name.last", reddo.TypeString); v != "Nguyen" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "Nguyen", v)
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
			if v := bo.GetExtraAttrAsUnsafe("email", reddo.TypeString); v != "myname@mydomain.com" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "myname@mydomain.com", v)
			}
			if v := bo.GetExtraAttrAsUnsafe("subject", reddo.TypeString); v != "English" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "English", v)
			}
			if v := bo.GetExtraAttrAsUnsafe("level", reddo.TypeString); v != "entry" {
				t.Fatalf("%s failed: expected %#v but received %#v", name, "entry", v)
			}
			if v := bo.GetExtraAttrAsUnsafe("age", reddo.TypeInt); v != int64(37) {
				t.Fatalf("%s failed: expected %#v but received %#v", name, int64(37), v)
			}
		}
	}
}

func TestDynamodb_SaveExistingUnique(t *testing.T) {
	name := "TestDynamodb_SaveExistingUnique"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	_cleanupDynamodb(adc, awsDynamodbTableNoUidx)
	_cleanupDynamodb(adc, awsDynamodbTableUidx)
	dao1 := _testDynamodbInit(t, name, adc, awsDynamodbTableNoUidx, nil)
	dao2 := _testDynamodbInit(t, name, adc, awsDynamodbTableUidx, [][]string{{"email"}, {"subject", "level"}})

	ubo1 := NewUniversalBo("1", 1357)
	ubo2 := NewUniversalBo("2", 1357)
	for i, ubo := range []*UniversalBo{ubo1, ubo2} {
		idStr := strconv.Itoa(i + 1)
		ubo.SetDataAttr("name.first", "Name-"+idStr)
		ubo.SetDataAttr("name.last", "Nguyen")
		ubo.SetExtraAttr("email", idStr+"@mydomain.com")
		ubo.SetExtraAttr("subject", "English").SetExtraAttr("level", idStr)
		ubo.SetExtraAttr("age", 35+i)
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

func TestDynamodb_CreateUpdateGet_Checksum(t *testing.T) {
	name := "TestDynamodb_CreateUpdateGet_Checksum"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()
	tableName := "table_temp"
	_cleanupDynamodb(adc, tableName)
	dao := _testDynamodbInit(t, name, adc, tableName, nil)

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
