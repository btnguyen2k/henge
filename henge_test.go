package henge

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
)

func Test_cloneMap_nil(t *testing.T) {
	name := "Test_cloneMap_nil"
	clone := cloneMap(nil)
	if clone != nil {
		t.Fatalf("%s failed: expected %#v but received %#v", name, nil, clone)
	}
}

func Test_cloneSlice_nil(t *testing.T) {
	name := "Test_cloneSlice_nil"
	clone := cloneSlice(nil)
	if clone != nil {
		t.Fatalf("%s failed: expected %#v but received %#v", name, nil, clone)
	}
}

func Test_cloneMap(t *testing.T) {
	name := "Test_cloneMap"
	type mystruct struct {
		str  string
		num  int
		bool bool
	}
	vstruct := mystruct{
		str:  "(struct) a string",
		num:  103,
		bool: true,
	}
	dmap := map[string]interface{}{"str": "(dmap) a string", "num": 3210, "bool": true}
	dslice := []interface{}{"(dslice) a string", 3010, true}
	vmap := map[string]interface{}{"str": "(map) a string", "num": 321, "bool": true, "map": dmap, "slice": dslice}
	vslice := []interface{}{"(slice) a string", 301, true, dmap, dslice}
	src := map[string]interface{}{
		"vstr":     "a string",
		"vnum":     12.34,
		"vbool":    true,
		"vstruct":  vstruct,
		"vpstruct": &vstruct,
		"vslice":   vslice,
		"vpslice":  &vslice,
		"vmap":     vmap,
		"vpmap":    &vmap,
	}
	dest := cloneMap(src)
	if !reflect.DeepEqual(src, dest) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
	}
	{
		old := src["vstr"]
		src["vstr"] = "another string"
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		src["vstr"] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := src["vnum"]
		src["vnum"] = 34.12
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		src["vnum"] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := src["vbool"]
		src["vbool"] = false
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		src["vbool"] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dmap["str"]
		dmap["str"] = "(map) another string"
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dmap["str"] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dmap["num"]
		dmap["num"] = 999
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dmap["num"] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dmap["bool"]
		dmap["bool"] = false
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dmap["bool"] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dslice[0]
		dslice[0] = "(slice) another string"
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dslice[0] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dslice[1]
		dslice[1] = 9999
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dslice[1] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dslice[2]
		dslice[2] = false
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dslice[2] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
}

func Test_cloneSlice(t *testing.T) {
	name := "Test_cloneSlice"
	type mystruct struct {
		str  string
		num  int
		bool bool
	}
	vstruct := mystruct{
		str:  "(struct) a string",
		num:  103,
		bool: true,
	}
	dmap := map[string]interface{}{"str": "(dmap) a string", "num": 3210, "bool": true}
	dslice := []interface{}{"(dslice) a string", 3010, true}
	vmap := map[string]interface{}{"str": "(map) a string", "num": 321, "bool": true, "map": dmap, "slice": dslice}
	vslice := []interface{}{"(slice) a string", 301, true, dmap, dslice}
	src := []interface{}{"a string", 12.34, true, vstruct, &vstruct, vslice, &vslice, vmap, &vmap}
	dest := cloneSlice(src)
	if !reflect.DeepEqual(src, dest) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
	}
	{
		old := src[0]
		src[0] = "another string"
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		src[0] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := src[1]
		src[1] = 34.12
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		src[1] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := src[2]
		src[2] = false
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		src[2] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dmap["str"]
		dmap["str"] = "(map) another string"
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dmap["str"] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dmap["num"]
		dmap["num"] = 999
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dmap["num"] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dmap["bool"]
		dmap["bool"] = false
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dmap["bool"] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dslice[0]
		dslice[0] = "(slice) another string"
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dslice[0] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dslice[1]
		dslice[1] = 9999
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dslice[1] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
	{
		old := dslice[2]
		dslice[2] = false
		if reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected src/dest are different", name)
		}
		dslice[2] = old
		if !reflect.DeepEqual(src, dest) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, src, dest)
		}
	}
}

func TestNewUniversalBo(t *testing.T) {
	name := "TestNewUniversalBo"
	ubo := NewUniversalBo("id", 1357)
	if ubo == nil {
		t.Fatalf("%s failed: nil", name)
	}
	if id := ubo.GetId(); id != "id" {
		t.Fatalf("%s failed: expected bo's id to be %#v but received %#v", name, "id", id)
	}
	if appVersion := ubo.GetTagVersion(); appVersion != 1357 {
		t.Fatalf("%s failed: expected bo's id to be %#v but received %#v", name, 1357, appVersion)
	}
}

func TestUniversalBo_ToMap(t *testing.T) {
	name := "TestUniversalBo_ToMap"
	ubo := NewUniversalBo("id", 1357)
	m := ubo.ToMap(nil, nil)
	if m == nil {
		t.Fatalf("%s failed: nil", name)
	}
	if m[FieldId] != "id" {
		t.Fatalf("%s failed: expected field %s has value %#v but received %#v", name, FieldId, "id", m[FieldId])
	}
	if m[FieldTagVersion] != uint64(1357) {
		t.Fatalf("%s failed: expected field %s has value %#v but received %#v", name, FieldTagVersion, 1357, m[FieldTagVersion])
	}
}

func TestUniversalBo_datatypes(t *testing.T) {
	name := "TestUniversalBo_datatypes"
	ubo := NewUniversalBo("id", 1357)
	vInt := 123
	ubo.SetDataAttr("data.number[0]", vInt)
	vFloat := 45.6
	ubo.SetDataAttr("data.number[1]", vFloat)
	vBool := true
	ubo.SetDataAttr("data.bool", vBool)
	vString := "a string"
	ubo.SetDataAttr("data.string", vString)
	vTime := time.Now()
	ubo.SetDataAttr("data.time[0]", vTime)
	ubo.SetDataAttr("data.time[1]", vTime.Format(TimeLayout))

	if v, err := ubo.GetDataAttrAs("data.number[0]", reddo.TypeInt); err != nil {
		t.Fatalf("%s failed: %#e", name, err)
	} else if v != int64(vInt) {
		t.Fatalf("%s failed [int]: expected %#v but received %#v", name, vInt, v)
	}
	if v, err := ubo.GetDataAttrAs("data.number[0]", reddo.TypeUint); err != nil {
		t.Fatalf("%s failed: %#e", name, err)
	} else if v != uint64(vInt) {
		t.Fatalf("%s failed [uint]: expected %#v but received %#v", name, vInt, v)
	}
	if v, err := ubo.GetDataAttrAs("data.number[1]", reddo.TypeFloat); err != nil {
		t.Fatalf("%s failed: %#e", name, err)
	} else if v != float64(vFloat) {
		t.Fatalf("%s failed [float]: expected %#v but received %#v", name, vFloat, v)
	}
	if v, err := ubo.GetDataAttrAs("data.bool", reddo.TypeBool); err != nil {
		t.Fatalf("%s failed: %#e", name, err)
	} else if v != vBool {
		t.Fatalf("%s failed [bool]: expected %#v but received %#v", name, vBool, v)
	}
	if v, err := ubo.GetDataAttrAs("data.string", reddo.TypeString); err != nil {
		t.Fatalf("%s failed: %#e", name, err)
	} else if v != vString {
		t.Fatalf("%s failed [string]: expected %#v but received %#v", name, vString, v)
	}
	if v, err := ubo.GetDataAttrAsTimeWithLayout("data.time[0]", TimeLayout); err != nil {
		t.Fatalf("%s failed: %#e", name, err)
	} else if v.Format(TimeLayout) != vTime.Format(TimeLayout) {
		t.Fatalf("%s failed [time]: expected %#v but received %#v", name, vTime, v)
	}
	if v, err := ubo.GetDataAttrAsTimeWithLayout("data.time[1]", TimeLayout); err != nil {
		t.Fatalf("%s failed: %#e", name, err)
	} else if v.Format(TimeLayout) != vTime.Format(TimeLayout) {
		t.Fatalf("%s failed [time]: expected %#v but received %#v", name, vTime, v)
	}
}

func TestUniversalBo_json(t *testing.T) {
	name := "TestUniversalBo_json"
	ubo1 := NewUniversalBo("id", 1357)
	vInt := float64(123)
	ubo1.SetDataAttr("data.number[0]", vInt)
	vFloat := 45.6
	ubo1.SetDataAttr("data.number[1]", vFloat)
	vBool := true
	ubo1.SetDataAttr("data.bool", vBool)
	vString := "a string"
	ubo1.SetDataAttr("data.string", vString)
	vTime := time.Now()
	ubo1.SetDataAttr("data.time[0]", vTime.Format(TimeLayout))
	ubo1.SetDataAttr("data.time[1]", vTime.Format(TimeLayout))
	js1, _ := json.Marshal(ubo1)

	var ubo2 *UniversalBo
	err := json.Unmarshal(js1, &ubo2)
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	if ubo1.id != ubo2.id {
		t.Fatalf("%s failed [id]: expected %#v but received %#v", name, ubo1.id, ubo2.id)
	}
	if ubo1.tagVersion != ubo2.tagVersion {
		t.Fatalf("%s failed [appversion]: expected %#v but received %#v", name, ubo1.tagVersion, ubo2.tagVersion)
	}
	if !reflect.DeepEqual(ubo1._data, ubo2._data) {
		t.Fatalf("%s failed [data]: expected\n%#v\nbut received\n%#v", name, ubo1._data, ubo2._data)
	}
	if ubo1.checksum != ubo2.checksum {
		t.Fatalf("%s failed [checksum]: expected %#v but received %#v", name, ubo1.checksum, ubo2.checksum)
	}
}

func TestUniversalBo_SetId(t *testing.T) {
	name := "TestUniversalBo_json"
	ubo := NewUniversalBo("id", 1357)
	id := "  This IS an Id  "
	ubo.SetId(id)
	id = strings.TrimSpace(id)
	if id != ubo.GetId() {
		t.Fatalf("%s failed: expected %#v but received %#v", name, id, ubo.GetId())
	}
}

func TestUniversalBo_SetDataJson(t *testing.T) {
	name := "TestUniversalBo_SetDataJson"
	ubo := NewUniversalBo("id", 1357)
	jsonData := `{"a":"a string","b":1,"c":true}`
	ubo.SetDataJson(jsonData)
	if ubo.GetDataJson() != jsonData {
		t.Fatalf("%s failed: expected %#v but received %#v", name, jsonData, ubo.GetDataJson())
	}
}

func TestUniversalBo_SetTagVersion(t *testing.T) {
	name := "TestUniversalBo_SetTagVersion"
	ubo := NewUniversalBo("id", 1357)
	ubo.SetTagVersion(1234)
	if ubo.GetTagVersion() != uint64(1234) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, 1234, ubo.GetTagVersion())
	}
}

func TestUniversalBo_GetTimeCreated(t *testing.T) {
	name := "TestUniversalBo_GetTimeCreated"
	now := time.Now()
	ubo := NewUniversalBo("id", 1357)
	if d := ubo.GetTimeCreated().Nanosecond() - now.Nanosecond(); d > 10000 {
		t.Fatalf("%s failed: expected delta less than %#v but received %#v", name, 10000, d)
	}
}

func TestUniversalBo_SetTimeUpdated(t *testing.T) {
	name := "TestUniversalBo_SetTimeUpdated"
	ubo := NewUniversalBo("id", 1357)
	tupdate := time.Now().Add(5 * time.Minute)
	ubo.SetTimeUpdated(tupdate)
	if ubo.GetTimeUpdated().Nanosecond() != tupdate.Nanosecond() {
		t.Fatalf("%s failed: expected %#v but received %#v", name, tupdate.Nanosecond(), ubo.GetTimeUpdated().Nanosecond())
	}
}

func TestUniversalBo_SetExtraAttr(t *testing.T) {
	name := "TestUniversalBo_SetExtraAttr"
	ubo := NewUniversalBo("id", 1357)
	now := time.Now()
	ubo.SetExtraAttr("str", "a string")
	ubo.SetExtraAttr("int", 123)
	ubo.SetExtraAttr("b", true)
	ubo.SetExtraAttr("dstr", now.Format(TimeLayout))
	ubo.SetExtraAttr("d", &now)
	fields := []string{"str", "int", "b", "dstr", "d"}
	m := ubo.GetExtraAttrs()
	for _, f := range fields {
		if _, ok := m[f]; !ok {
			t.Fatalf("%s failed: field %s does not exist", name, f)
		}
		if ubo.GetExtraAttr(f) == nil {
			t.Fatalf("%s failed: field %s does not exist", name, f)
		}
	}
	if v, err := ubo.GetExtraAttrAs("str", reddo.TypeString); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if v != "a string" {
		t.Fatalf("%s failed: expected %#v but received %#v", name, "a string", v)
	}
	if v := ubo.GetExtraAttrAsUnsafe("int", reddo.TypeInt); v != int64(123) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, 123, v)
	}
	if v, err := ubo.GetExtraAttrAsTimeWithLayout("d", TimeLayout); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if v.Second() != now.Second() {
		t.Fatalf("%s failed: expected %#v but received %#v", name, now.Format(TimeLayout), v.Format(TimeLayout))
	}
	if v, err := ubo.GetExtraAttrAsTimeWithLayout("dstr", TimeLayout); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if v.Second() != now.Second() {
		t.Fatalf("%s failed: expected %#v but received %#v", name, now, v)
	}
	if v := ubo.GetExtraAttrAsTimeWithLayoutUnsafe("d", TimeLayout); v.Second() != now.Second() {
		t.Fatalf("%s failed: expected %#v but received %#v", name, now, v)
	}
	if v := ubo.GetExtraAttrAsTimeWithLayoutUnsafe("dstr", TimeLayout); v.Second() != now.Second() {
		t.Fatalf("%s failed: expected %#v but received %#v", name, now, v)
	}
}

func TestUniversalBo_SetDataAttr(t *testing.T) {
	name := "TestUniversalBo_SetDataAttr"
	ubo := NewUniversalBo("id", 1357)
	now := time.Now()
	ubo.SetDataAttr("s.t.r.str", "a string")
	ubo.SetDataAttr("i[0].int", 123)
	ubo.SetDataAttr("b", true)
	ubo.SetDataAttr("time[0]", now.Format(TimeLayout))
	ubo.SetDataAttr("time[1]", &now)
	fields := []string{"s.t.r.str", "i[0].int", "b", "time[0]", "time[1]"}
	for _, f := range fields {
		if v, err := ubo.GetDataAttr(f); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if v == nil {
			t.Fatalf("%s failed: path %s does not exist", name, f)
		}
		if ubo.GetDataAttrUnsafe(f) == nil {
			t.Fatalf("%s failed: path %s does not exist", name, f)
		}
	}
	if v, err := ubo.GetDataAttrAs("s.t.r.str", reddo.TypeString); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if v != "a string" {
		t.Fatalf("%s failed: expected %#v but received %#v", name, "a string", v)
	}
	if v := ubo.GetDataAttrAsUnsafe("i[0].int", reddo.TypeInt); v != int64(123) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, 123, v)
	}
	if v, err := ubo.GetDataAttrAsTimeWithLayout("time[0]", TimeLayout); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if v.Second() != now.Second() {
		t.Fatalf("%s failed: expected %#v but received %#v", name, now.Format(TimeLayout), v.Format(TimeLayout))
	}
	if v, err := ubo.GetDataAttrAsTimeWithLayout("time[1]", TimeLayout); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if v.Second() != now.Second() {
		t.Fatalf("%s failed: expected %#v but received %#v", name, now, v)
	}
	if v := ubo.GetDataAttrAsTimeWithLayoutUnsafe("time[0]", TimeLayout); v.Second() != now.Second() {
		t.Fatalf("%s failed: expected %#v but received %#v", name, now, v)
	}
	if v := ubo.GetDataAttrAsTimeWithLayoutUnsafe("time[1]", TimeLayout); v.Second() != now.Second() {
		t.Fatalf("%s failed: expected %#v but received %#v", name, now, v)
	}
}

func TestUniversalBo_Checksum(t *testing.T) {
	name := "TestUniversalBo_Checksum"
	ubo := NewUniversalBo("id", 1357)
	checksum1 := ubo.GetChecksum()
	ubo.SetDataAttr("str", "a string")
	if !ubo.IsDirty() {
		t.Fatalf("%s failed: expected bo is dirty", name)
	}
	ubo.Sync()
	checksum2 := ubo.GetChecksum()
	if ubo.IsDirty() {
		t.Fatalf("%s failed: expected bo is not dirty", name)
	}
	if checksum1 == checksum2 {
		t.Fatalf("%s failed", name)
	}
}

func TestUniversalBo_Checksum2(t *testing.T) {
	name := "TestUniversalBo_Checksum2"
	ubo := NewUniversalBo("id", 1357)
	checksum1 := ubo.GetChecksum()
	ubo.SetExtraAttr("str", "a string")
	if !ubo.IsDirty() {
		t.Fatalf("%s failed: expected bo is dirty", name)
	}
	ubo.Sync()
	checksum2 := ubo.GetChecksum()
	if ubo.IsDirty() {
		t.Fatalf("%s failed: expected bo is not dirty", name)
	}
	if checksum1 == checksum2 {
		t.Fatalf("%s failed", name)
	}
}

func TestRowMapper(t *testing.T) {
	name := "TestRowMapper"
	tableName := "test_user"
	extraColNameToFieldMappings := map[string]string{"zuid": "owner_id"}
	rowMapper := buildRowMapperSql(tableName, extraColNameToFieldMappings)

	myColList := rowMapper.ColumnsList(tableName)
	expectedColList := append(sqlColumnNames, "zuid")
	if !reflect.DeepEqual(myColList, expectedColList) {
		t.Fatalf("%s failed: expected column list %#v but received %#v", name, expectedColList, myColList)
	}
}

type testMyBo struct {
	*UniversalBo
	Name string
}

func (bo *testMyBo) Sync(opts ...UboSyncOpts) *testMyBo {
	bo.SetDataAttr("name", bo.Name)
	bo.UniversalBo.Sync(opts...)
	return bo
}

func _newMyBoFromUbo(ubo *UniversalBo) *testMyBo {
	if ubo == nil {
		return nil
	}
	ubo = ubo.Clone()
	bo := &testMyBo{UniversalBo: ubo}
	if v, err := ubo.GetDataAttrAs("name", reddo.TypeString); err != nil {
		return nil
	} else {
		bo.Name, _ = v.(string)
	}
	return bo.Sync()
}

func TestBuildBoFromUbo_PreserveTimestamp(t *testing.T) {
	name := "TestBuildBoFromUbo_PreserveTimestamp"
	TimestampRounding = TimestampRoundSettingNone
	gbo := godal.NewGenericBo()
	_now := time.Now()
	_next := _now.Add(7 * time.Second)
	gbo.GboSetAttr(FieldTimeCreated, _now)
	gbo.GboSetAttr(FieldTimeUpdated, _next)
	gbo.GboSetAttr(FieldChecksum, "")
	_id := "1"
	_tag := 1024
	gbo.GboSetAttr(FieldId, _id)
	gbo.GboSetAttr(FieldTagVersion, _tag)
	gbo.GboSetAttr(FieldData, `{"key":"value"}`)

	// fmt.Println(_now, "/", _next)
	ubo := NewUniversalBoFromGbo(gbo)
	if ubo == nil {
		t.Fatalf("%s failed: nil", name)
	}
	// fmt.Println(ubo.GetTimeCreated(), "/", ubo.GetTimeUpdated())
	if !_now.Equal(ubo.GetTimeCreated()) {
		t.Fatalf("%s failed - expected UBO timeCreated %s but received %s", name, _now, ubo.GetTimeCreated())
	}
	if !_next.Equal(ubo.GetTimeUpdated()) {
		t.Fatalf("%s failed - expected UBO timeUpdated %s but received %s", name, _next, ubo.GetTimeUpdated())
	}

	bo := _newMyBoFromUbo(ubo)
	if bo == nil {
		t.Fatalf("%s failed: nil", name)
	}
	// fmt.Println(bo.GetTimeCreated(), "/", bo.GetTimeUpdated())
	if !_now.Equal(bo.GetTimeCreated()) {
		t.Fatalf("%s failed - expected BO timeCreated %s but received %s", name, _now, bo.GetTimeCreated())
	}
	if !_next.Equal(bo.GetTimeUpdated()) {
		t.Fatalf("%s failed - expected BO timeUpdated %s but received %s", name, _next, bo.GetTimeUpdated())
	}

	bo.Sync()
	if !_now.Equal(bo.GetTimeCreated()) {
		t.Fatalf("%s failed - expected BO timeCreated %s but received %s", name, _now, bo.GetTimeCreated())
	}
	if !_next.Equal(bo.GetTimeUpdated()) {
		t.Fatalf("%s failed - expected BO timeUpdated %s but received %s", name, _next, bo.GetTimeUpdated())
	}

	bo.Sync(UboSyncOpts{UpdateTimestampIfChecksumChange: true})
	if !_now.Equal(bo.GetTimeCreated()) {
		t.Fatalf("%s failed - expected BO timeCreated %s but received %s", name, _now, bo.GetTimeCreated())
	}
	if !_next.Equal(bo.GetTimeUpdated()) {
		t.Fatalf("%s failed - expected BO timeUpdated %s but received %s", name, _next, bo.GetTimeUpdated())
	}

	bo.Name += "-traling"
	now := time.Now()
	bo.Sync(UboSyncOpts{UpdateTimestampIfChecksumChange: true})
	if !_now.Equal(bo.GetTimeCreated()) {
		t.Fatalf("%s failed - expected BO timeCreated %s but received %s", name, _now, bo.GetTimeCreated())
	}
	if bo.GetTimeUpdated().Before(now) {
		t.Fatalf("%s failed - expected BO timeUpdated %s but received %s", name, now, bo.GetTimeUpdated())
	}
}
