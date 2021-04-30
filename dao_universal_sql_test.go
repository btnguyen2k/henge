package henge

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

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

	input := NewUniversalBo("myid", 1234)
	expected := map[string]interface{}{SqlColId: "myid"}
	if filter := defaultFilterGeneratorSql("", input); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}
	if filter := defaultFilterGeneratorSql("", *input); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}

	input2 := godal.NewGenericBo()
	input2.GboSetAttr(FieldId, "myid2")
	expected = map[string]interface{}{SqlColId: "myid2"}
	if filter := defaultFilterGeneratorSql("", input2); !reflect.DeepEqual(filter, expected) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, filter)
	}

	input3 := map[string]interface{}{"filter": "value"}
	expected = map[string]interface{}{"filter": "value"}
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

// User is the business object
//	- User inherits unique id from bo.UniversalBo
//
// available since template-v0.2.0
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
