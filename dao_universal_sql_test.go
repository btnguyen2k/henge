package henge

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	_ "github.com/btnguyen2k/gocosmos"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
)

func TestNewSqlConnection(t *testing.T) {
	name := "TestNewSqlConnection"

	url := "db://invalid"
	tz := "UTC"
	drv := "invalid"
	timeout := 0
	var poolOpt *prom.SqlPoolOptions = nil
	if sqlc, err := NewSqlConnection(url, tz, drv, prom.FlavorDefault, timeout, poolOpt); err == nil || sqlc != nil {
		t.Fatalf("%s failed: expecting nil/error but received %#v/%s", name, sqlc, err)
	}

	url = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
	tz = "invalid"
	drv = "gocosmos"
	if sqlc, err := NewSqlConnection(url, tz, drv, prom.FlavorDefault, timeout, poolOpt); err != nil || sqlc == nil {
		t.Fatalf("%s failed: %#v/%s", name, sqlc, err)
	}
}

func Test_DefaultFilterGeneratorSql(t *testing.T) {
	name := "Test_DefaultFilterGeneratorSql"

	var expected godal.FilterOpt
	input := NewUniversalBo("myid", 1234)
	expected = &godal.FilterOptFieldOpValue{FieldName: FieldId, Operator: godal.FilterOpEqual, Value: "myid"}
	if filter := defaultFilterGeneratorSql("", input); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}
	if filter := defaultFilterGeneratorSql("", *input); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}

	input2 := godal.NewGenericBo()
	input2.GboSetAttr(FieldId, "myid2")
	expected = &godal.FilterOptFieldOpValue{FieldName: FieldId, Operator: godal.FilterOpEqual, Value: "myid2"}
	if filter := defaultFilterGeneratorSql("", input2); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}

	input3 := godal.MakeFilter(map[string]interface{}{FieldId: "myid3"})
	expected = &godal.FilterOptFieldOpValue{FieldName: FieldId, Operator: godal.FilterOpEqual, Value: "myid3"}
	if filter := defaultFilterGeneratorSql("", input3); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}
}

/*----------------------------------------------------------------------*/

func newUser(appVersion uint64, id, maskId string) *User {
	user := &User{
		UniversalBo: *NewUniversalBo(id, appVersion),
	}
	return user.SetMaskId(maskId).sync()
}

func newUserFromUbo(ubo *UniversalBo) *User {
	if ubo == nil {
		return nil
	}
	ubo = ubo.Clone()
	user := &User{UniversalBo: *ubo}
	{
		v, err := ubo.GetDataAttrAs(userAttrMaskId, reddo.TypeString)
		if err != nil {
			return nil
		}
		user.maskId, _ = v.(string)
	}
	{
		v, err := ubo.GetDataAttrAs(userAttrDisplayName, reddo.TypeString)
		if err != nil {
			return nil
		}
		user.displayName, _ = v.(string)
	}
	{
		v, err := ubo.GetDataAttrAs(userAttrIsAdmin, reddo.TypeBool)
		if err != nil {
			return nil
		}
		user.isAdmin, _ = v.(bool)
	}
	{
		v, err := ubo.GetDataAttrAs(userAttrPassword, reddo.TypeString)
		if err != nil {
			return nil
		}
		user.password, _ = v.(string)
	}
	return user.sync()
}

const (
	userAttrMaskId      = "mid"
	userAttrPassword    = "pwd"
	userAttrDisplayName = "dname"
	userAttrIsAdmin     = "isadm"
	userAttrUbo         = "_ubo"
)

type User struct {
	UniversalBo `json:"_ubo"`
	maskId      string `json:"mid"`
	password    string `json:"pwd"`
	displayName string `json:"dname"`
	isAdmin     bool   `json:"isadm"`
}

func (u *User) ToMap(postFunc FuncPostUboToMap) map[string]interface{} {
	result := map[string]interface{}{
		FieldId:             u.GetId(),
		userAttrMaskId:      u.maskId,
		userAttrIsAdmin:     u.isAdmin,
		userAttrDisplayName: u.displayName,
	}
	if postFunc != nil {
		result = postFunc(result)
	}
	return result
}

func (u *User) MarshalJSON() ([]byte, error) {
	u.sync()
	m := map[string]interface{}{
		userAttrUbo: u.UniversalBo.Clone(),
		"_cols": map[string]interface{}{
			userAttrMaskId: u.maskId,
		},
		"_attrs": map[string]interface{}{
			userAttrDisplayName: u.displayName,
			userAttrIsAdmin:     u.isAdmin,
			userAttrPassword:    u.password,
		},
	}
	return json.Marshal(m)
}

func (u *User) UnmarshalJSON(data []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	var err error
	if m[userAttrUbo] != nil {
		js, _ := json.Marshal(m[userAttrUbo])
		if err = json.Unmarshal(js, &u.UniversalBo); err != nil {
			return err
		}
	}
	if _cols, ok := m["_cols"].(map[string]interface{}); ok {
		if u.maskId, err = reddo.ToString(_cols[userAttrMaskId]); err != nil {
			return err
		}
	}
	if _attrs, ok := m["_attrs"].(map[string]interface{}); ok {
		if u.displayName, err = reddo.ToString(_attrs[userAttrDisplayName]); err != nil {
			return err
		}
		if u.isAdmin, err = reddo.ToBool(_attrs[userAttrIsAdmin]); err != nil {
			return err
		}
		if u.password, err = reddo.ToString(_attrs[userAttrPassword]); err != nil {
			return err
		}
	}
	u.sync()
	return nil
}

func (u *User) GetMaskId() string {
	return u.maskId
}

func (u *User) SetMaskId(v string) *User {
	u.maskId = strings.TrimSpace(strings.ToLower(v))
	return u
}

func (u *User) GetPassword() string {
	return u.password
}

func (u *User) SetPassword(v string) *User {
	u.password = strings.TrimSpace(v)
	return u
}

func (u *User) GetDisplayName() string {
	return u.displayName
}

func (u *User) SetDisplayName(v string) *User {
	u.displayName = strings.TrimSpace(v)
	return u
}

func (u *User) IsAdmin() bool {
	return u.isAdmin
}

func (u *User) SetAdmin(v bool) *User {
	u.isAdmin = v
	return u
}

func (u *User) sync() *User {
	u.SetDataAttr(userAttrPassword, u.password)
	u.SetDataAttr(userAttrDisplayName, u.displayName)
	u.SetDataAttr(userAttrIsAdmin, u.isAdmin)
	u.SetDataAttr(userAttrMaskId, u.maskId)
	u.UniversalBo.Sync()
	return u
}

/*----------------------------------------------------------------------*/

var testSqlList = []string{"mssql", "mysql", "pgsql", "oracle", "sqlite"}
var testSqlSetupFuncMap = map[string]TestSetupOrTeardownFunc{
	"mssql":  setupTestMssql,
	"mysql":  setupTestMysql,
	"oracle": setupTestOracle,
	"pgsql":  setupTestPgsql,
	"sqlite": setupTestSqlite,
}
var testSqlTeardownFuncMap = map[string]TestSetupOrTeardownFunc{
	"mssql":  teardownTestMssql,
	"mysql":  teardownTestMysql,
	"oracle": teardownTestOracle,
	"pgsql":  teardownTestPgsql,
	"sqlite": teardownTestSqlite,
}

func TestUniversalDaoSql_Create(t *testing.T) {
	testName := "TestUniversalDaoSql_Create"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_CreateExistingPK(t *testing.T) {
	testName := "TestUniversalDaoSql_CreateExistingPK"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_CreateExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoSql_CreateExistingUnique"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_CreateGet(t *testing.T) {
	testName := "TestUniversalDaoSql_CreateGet"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_CreateDelete(t *testing.T) {
	testName := "TestUniversalDaoSql_CreateDelete"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_CreateGetMany(t *testing.T) {
	testName := "TestUniversalDaoSql_CreateGetMany"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_CreateGetManyWithFilter(t *testing.T) {
	testName := "TestUniversalDaoSql_CreateGetManyWithFilter"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_CreateGetManyWithSorting(t *testing.T) {
	testName := "TestUniversalDaoSql_CreateGetManyWithSorting"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_CreateGetManyWithFilterAndSorting(t *testing.T) {
	testName := "TestUniversalDaoSql_CreateGetManyWithFilterAndSorting"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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

func TestUniversalDaoSql_CreateGetManyWithSortingAndPaging(t *testing.T) {
	testName := "TestUniversalDaoSql_CreateGetManyWithSortingAndPaging"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_Update(t *testing.T) {
	testName := "TestUniversalDaoSql_Update"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_UpdateNotExist(t *testing.T) {
	testName := "TestUniversalDaoSql_UpdateNotExist"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_UpdateDuplicated(t *testing.T) {
	testName := "TestUniversalDaoSql_UpdateDuplicated"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_SaveNew(t *testing.T) {
	testName := "TestUniversalDaoSql_SaveNew"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_SaveExisting(t *testing.T) {
	testName := "TestUniversalDaoSql_SaveExisting"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_SaveExistingUnique(t *testing.T) {
	testName := "TestUniversalDaoSql_SaveExistingUnique"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}

func TestUniversalDaoSql_CreateUpdateGet_Checksum(t *testing.T) {
	testName := "TestUniversalDaoSql_CreateUpdateGet_Checksum"
	for _, subtest := range testSqlList {
		t.Run(subtest, func(t *testing.T) {
			setupFunc := testSqlSetupFuncMap[subtest]
			teardownFunc := testSqlTeardownFuncMap[subtest]
			teardownTest := setupTest(t, testName, setupFunc, teardownFunc)
			defer teardownTest(t)
			if testDao == nil {
				t.Skip("skipped.")
			}

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
		})
	}
}
