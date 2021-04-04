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
	if v, err := ubo.GetDataAttrAs(userAttr_MaskId, reddo.TypeString); err != nil {
		return nil
	} else {
		user.maskId, _ = v.(string)
	}
	if v, err := ubo.GetDataAttrAs(userAttr_DisplayName, reddo.TypeString); err != nil {
		return nil
	} else {
		user.displayName, _ = v.(string)
	}
	if v, err := ubo.GetDataAttrAs(userAttr_IsAdmin, reddo.TypeBool); err != nil {
		return nil
	} else {
		user.isAdmin, _ = v.(bool)
	}
	if v, err := ubo.GetDataAttrAs(userAttr_Password, reddo.TypeString); err != nil {
		return nil
	} else {
		user.password, _ = v.(string)
	}
	return user.sync()
}

const (
	userAttr_MaskId      = "mid"
	userAttr_Password    = "pwd"
	userAttr_DisplayName = "dname"
	userAttr_IsAdmin     = "isadm"
	userAttr_Ubo         = "_ubo"
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
		FieldId:              u.GetId(),
		userAttr_MaskId:      u.maskId,
		userAttr_IsAdmin:     u.isAdmin,
		userAttr_DisplayName: u.displayName,
	}
	if postFunc != nil {
		result = postFunc(result)
	}
	return result
}

func (u *User) MarshalJSON() ([]byte, error) {
	u.sync()
	m := map[string]interface{}{
		userAttr_Ubo: u.UniversalBo.Clone(),
		"_cols": map[string]interface{}{
			userAttr_MaskId: u.maskId,
		},
		"_attrs": map[string]interface{}{
			userAttr_DisplayName: u.displayName,
			userAttr_IsAdmin:     u.isAdmin,
			userAttr_Password:    u.password,
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
	if m[userAttr_Ubo] != nil {
		js, _ := json.Marshal(m[userAttr_Ubo])
		if err = json.Unmarshal(js, &u.UniversalBo); err != nil {
			return err
		}
	}
	if _cols, ok := m["_cols"].(map[string]interface{}); ok {
		if u.maskId, err = reddo.ToString(_cols[userAttr_MaskId]); err != nil {
			return err
		}
	}
	if _attrs, ok := m["_attrs"].(map[string]interface{}); ok {
		if u.displayName, err = reddo.ToString(_attrs[userAttr_DisplayName]); err != nil {
			return err
		}
		if u.isAdmin, err = reddo.ToBool(_attrs[userAttr_IsAdmin]); err != nil {
			return err
		}
		if u.password, err = reddo.ToString(_attrs[userAttr_Password]); err != nil {
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
	u.SetDataAttr(userAttr_Password, u.password)
	u.SetDataAttr(userAttr_DisplayName, u.displayName)
	u.SetDataAttr(userAttr_IsAdmin, u.isAdmin)
	u.SetDataAttr(userAttr_MaskId, u.maskId)
	u.UniversalBo.Sync()
	return u
}
