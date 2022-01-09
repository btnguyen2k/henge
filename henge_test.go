package henge

import (
	"encoding/json"
	"fmt"
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
	_id := "id"
	_tagVersion := uint64(1357)
	ubo := NewUniversalBo(_id, _tagVersion)
	if ubo == nil {
		t.Fatalf("%s failed: nil", name)
	}
	if vE, vA := _id, ubo.GetId(); vE != vA {
		t.Fatalf("%s failed: expected bo's id to be %#v but received %#v", name, vE, vA)
	}
	if vE, vA := _tagVersion, ubo.GetTagVersion(); vE != vA {
		t.Fatalf("%s failed: expected bo's tag-version to be %#v but received %#v", name, vE, vA)
	}
}

func TestUniversalBo_ToMap(t *testing.T) {
	testName := "TestUniversalBo_ToMap"
	_id := "id"
	_tagVersion := uint64(1357)
	roundingOptList := []TimestampRoundSetting{TimestampRoundSettingNone, TimestampRoundSettingNanosecond, TimestampRoundSettingMicrosecond, TimestampRoundSettingMillisecond, TimestampRoundSettingSecond}
	expectedDeltaLimit1List := []int{0, -0, -999, -999_999, -999_999_999}
	expectedDeltaLimit2List := []int{999, 999, 999, 999_999, 999_999}
	vStr := "a string"
	vInt := 123
	vFloat := 4.56
	vBool := true
	for i, roundingOpt := range roundingOptList {
		t.Run(fmt.Sprintf("%v", roundingOpt), func(t *testing.T) {
			now := time.Now()
			ubo := NewUniversalBo(_id, _tagVersion, UboOpt{TimestampRounding: roundingOpt})
			ubo.SetDataAttr("key", "value")
			ubo.SetExtraAttr("str", vStr)
			ubo.SetExtraAttr("int", vInt)
			ubo.SetExtraAttr("float", vFloat)
			ubo.SetExtraAttr("bool", vBool)
			ubo.SetExtraAttr("t", now)
			ubo.Sync()
			next := now.Add(1024 * time.Millisecond)
			ubo.SetTimeUpdated(next)
			m := ubo.ToMap(nil, nil)
			if m == nil {
				t.Fatalf("%s failed: nil", testName)
			}
			if m[FieldId] != _id {
				t.Fatalf("%s failed: expected field %s has value %#v but received %#v", testName, FieldId, _id, m[FieldId])
			}
			if m[FieldTagVersion] != _tagVersion {
				t.Fatalf("%s failed: expected field %s has value %#v but received %#v", testName, FieldTagVersion, _tagVersion, m[FieldTagVersion])
			}
			if m[FieldData] != `{"key":"value"}` {
				t.Fatalf("%s failed: expected field %s has value %#v but received %#v", testName, FieldData, `{"key":"value"}`, m[FieldData])
			}
			if d := m[FieldTimeCreated].(time.Time).Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: expected delta between {%v - %v} but received %v", testName, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], d)
			}
			if d := m[FieldTimeUpdated].(time.Time).Nanosecond() - next.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: expected delta between {%v - %v} but received %v", testName, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], d)
			}

			mext, ok := m[FieldExtras].(map[string]interface{})
			if mext == nil || !ok {
				t.Fatalf("%s failed: invalid value for field %s", testName, FieldExtras)
			}
			if mext["str"] != vStr {
				t.Fatalf("%s failed: expected field %s has value %#v but received %#v", testName, FieldExtras+".str", vStr, mext["str"])
			}
			if mext["int"] != vInt {
				t.Fatalf("%s failed: expected field %s has value %#v but received %#v", testName, FieldExtras+".int", vInt, mext["int"])
			}
			if mext["float"] != vFloat {
				t.Fatalf("%s failed: expected field %s has value %#v but received %#v", testName, FieldExtras+".float", vFloat, mext["float"])
			}
			if mext["bool"] != vBool {
				t.Fatalf("%s failed: expected field %s has value %#v but received %#v", testName, FieldExtras+".bool", vBool, mext["bool"])
			}
			if d := mext["t"].(time.Time).Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: expected delta between {%v - %v} but received %v", testName, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], d)
			}
		})
	}
}

func TestUniversalBo_datatypes(t *testing.T) {
	testName := "TestUniversalBo_datatypes"
	_id := "id"
	_tagVersion := uint64(1357)
	fieldNameList := []string{"data.number[0]", "data.number[1]", "data.number[2]", "data.bool", "data.string"}
	fieldValueList := []interface{}{int64(123), uint64(456), float64(12.56), true, "a string"}
	fieldTypeList := []reflect.Type{reddo.TypeInt, reddo.TypeUint, reddo.TypeFloat, reddo.TypeBool, reddo.TypeString}
	roundingOptList := []TimestampRoundSetting{TimestampRoundSettingNone, TimestampRoundSettingNanosecond, TimestampRoundSettingMicrosecond, TimestampRoundSettingMillisecond, TimestampRoundSettingSecond}
	expectedDeltaLimit1List := []int{0, -0, -999, -999_999, -999_999_999}
	expectedDeltaLimit2List := []int{999, 999, 999, 999_999, 999_999}
	for i, roundingOpt := range roundingOptList {
		t.Run(fmt.Sprintf("%v", roundingOpt), func(t *testing.T) {
			ubo := NewUniversalBo(_id, _tagVersion, UboOpt{TimestampRounding: roundingOpt})
			for i, field := range fieldNameList {
				ubo.SetDataAttr(field, fieldValueList[i])
			}
			now := time.Now()
			ubo.SetDataAttr("data.time", now)

			for i, field := range fieldNameList {
				if v, err := ubo.GetDataAttrAs(field, fieldTypeList[i]); err != nil {
					t.Fatalf("%s failed: %#e", testName, err)
				} else if v != fieldValueList[i] {
					t.Fatalf("%s failed [field %s]: expected %#v but received %#v", testName, field, fieldValueList[i], v)
				}

				if v := ubo.GetDataAttrAsUnsafe(field, fieldTypeList[i]); v == nil {
					t.Fatalf("%s failed: nil", testName)
				} else if v != fieldValueList[i] {
					t.Fatalf("%s failed [field %s]: expected %#v but received %#v", testName, field, fieldValueList[i], v)
				}
			}

			if v, err := ubo.GetDataAttrAsTimeWithLayout("data.time", DefaultTimeLayout); err != nil {
				t.Fatalf("%s failed: %#e", testName, err)
			} else if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: expected delta between {%v - %v} but received %v", testName, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], d)
			}

			v := ubo.GetDataAttrAsTimeWithLayoutUnsafe("data.time", DefaultTimeLayout)
			if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: expected delta between {%v - %v} but received %v", testName, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], d)
			}
		})
	}
}

func TestUniversalBo_json(t *testing.T) {
	testName := "TestUniversalBo_json"
	_id := "id"
	_tagVersion := uint64(1357)
	fieldNameList := []string{"data.number[0]", "data.number[1]", "data.number[2]", "data.bool", "data.string"}
	fieldValueList := []interface{}{int64(123), uint64(456), float64(12.56), true, "a string"}
	fieldTypeList := []reflect.Type{reddo.TypeInt, reddo.TypeUint, reddo.TypeFloat, reddo.TypeBool, reddo.TypeString}
	roundingOptList := []TimestampRoundSetting{TimestampRoundSettingNone, TimestampRoundSettingNanosecond, TimestampRoundSettingMicrosecond, TimestampRoundSettingMillisecond, TimestampRoundSettingSecond}
	timeLayoutList := []string{"2006-01-02T15:04:05.999999999Z07:00", "2006-01-02T15:04:05.999999999Z07:00", "2006-01-02T15:04:05.999999Z07:00", "2006-01-02T15:04:05.999Z07:00", "2006-01-02T15:04:05Z07:00"}
	expectedDeltaLimit1List := []int{0, -0, -999, -999_999, -999_999_999}
	expectedDeltaLimit2List := []int{999, 999, 999, 999_999, 999_999}
	for i, roundingOpt := range roundingOptList {
		t.Run(fmt.Sprintf("%v", roundingOpt), func(t *testing.T) {
			ubo1 := NewUniversalBo(_id, _tagVersion, UboOpt{TimestampRounding: roundingOpt})
			for i, field := range fieldNameList {
				v := fieldValueList[i]
				switch v.(type) {
				case int:
					v = float64(v.(int))
				case int8:
					v = float64(v.(int8))
				case int16:
					v = float64(v.(int16))
				case int32:
					v = float64(v.(int32))
				case int64:
					v = float64(v.(int64))
				case uint:
					v = float64(v.(uint))
				case uint8:
					v = float64(v.(uint8))
				case uint16:
					v = float64(v.(uint16))
				case uint32:
					v = float64(v.(uint32))
				case uint64:
					v = float64(v.(uint64))
				case float32:
					v = float64(v.(float32))
				}
				ubo1.SetDataAttr(field, v)
			}
			now := time.Now()
			ubo1.SetDataAttr("data.time[0]", now)
			ubo1.SetDataAttr("data.time[1]", now.Format(time.RFC3339Nano))

			js1, _ := json.Marshal(ubo1)
			ubo2 := &UniversalBo{ /*_timeLayout: ubo1._timeLayout,*/ _timestampRounding: ubo1._timestampRounding}
			err := json.Unmarshal(js1, &ubo2)
			if err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			}

			if ubo2.id != _id {
				t.Fatalf("%s failed [id]: expected %#v but received %#v", testName, _id, ubo2.id)
			}
			if ubo2.tagVersion != _tagVersion {
				t.Fatalf("%s failed [appversion]: expected %#v but received %#v", testName, _tagVersion, ubo2.tagVersion)
			}

			for i, field := range fieldNameList {
				if v, err := ubo2.GetDataAttrAs(field, fieldTypeList[i]); err != nil {
					t.Fatalf("%s failed: %#e", testName, err)
				} else if v != fieldValueList[i] {
					t.Fatalf("%s failed [field %s]: expected %#v but received %#v", testName, field, fieldValueList[i], v)
				}

				if v := ubo2.GetDataAttrAsUnsafe(field, fieldTypeList[i]); v == nil {
					t.Fatalf("%s failed: nil", testName)
				} else if v != fieldValueList[i] {
					t.Fatalf("%s failed [field %s]: expected %#v but received %#v", testName, field, fieldValueList[i], v)
				}
			}

			// if v := ubo2.GetDataAttrUnsafe("data.time[0]"); true {
			// 	fmt.Printf("%T: %#v\n", v, v)
			// }
			// if v := ubo2.GetDataAttrUnsafe("data.time[1]"); true {
			// 	fmt.Printf("%T: %#v\n", v, v)
			// }

			if v, err := ubo2.GetDataAttrAsTimeWithLayout("data.time[0]", timeLayoutList[i]); err != nil {
				t.Fatalf("%s failed: %#e", testName, err)
			} else if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: expected delta between {%v - %v} but received %v", testName, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], d)
			}
			v := ubo2.GetDataAttrAsTimeWithLayoutUnsafe("data.time[0]", DefaultTimeLayout)
			if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: expected delta between {%v - %v} but received %v", testName, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], d)
			}

			if !reflect.DeepEqual(ubo1._data, ubo2._data) {
				// fmt.Printf("(%T) %#v / (%T) %#v / (%T) %#v\n",
				// 	ubo1.GetDataAttrUnsafe("data.number[0]"), ubo1.GetDataAttrUnsafe("data.number[0]"),
				// 	ubo1.GetDataAttrUnsafe("data.number[1]"), ubo1.GetDataAttrUnsafe("data.number[1]"),
				// 	ubo1.GetDataAttrUnsafe("data.number[2]"), ubo1.GetDataAttrUnsafe("data.number[2]"))
				// fmt.Printf("(%T) %#v / (%T) %#v / (%T) %#v\n",
				// 	ubo2.GetDataAttrUnsafe("data.number[0]"), ubo2.GetDataAttrUnsafe("data.number[0]"),
				// 	ubo2.GetDataAttrUnsafe("data.number[1]"), ubo2.GetDataAttrUnsafe("data.number[1]"),
				// 	ubo2.GetDataAttrUnsafe("data.number[2]"), ubo2.GetDataAttrUnsafe("data.number[2]"))

				t.Fatalf("%s failed [data]: expected\n%#v\nbut received\n%#v", testName, ubo1._data, ubo2._data)
			}
			if ubo1.checksum != ubo2.checksum {
				t.Fatalf("%s failed [checksum]: expected %#v but received %#v", testName, ubo1.checksum, ubo2.checksum)
			}
		})
	}
}

func TestUniversalBo_SetId(t *testing.T) {
	name := "TestUniversalBo_json"
	_id := "id"
	_tagVersion := uint64(1357)
	ubo := NewUniversalBo(_id, _tagVersion)
	id := "  This IS an Id  "
	ubo.SetId(id)
	id = strings.TrimSpace(id)
	if id != ubo.GetId() {
		t.Fatalf("%s failed: expected %#v but received %#v", name, id, ubo.GetId())
	}
}

func TestUniversalBo_SetDataJson(t *testing.T) {
	name := "TestUniversalBo_SetDataJson"
	_id := "id"
	_tagVersion := uint64(1357)
	ubo := NewUniversalBo(_id, _tagVersion)
	jsonData := `{"a":"a string","b":1,"c":true}`
	ubo.SetDataJson(jsonData)
	if ubo.GetDataJson() != jsonData {
		t.Fatalf("%s failed: expected %#v but received %#v", name, jsonData, ubo.GetDataJson())
	}
}

func TestUniversalBo_SetTagVersion(t *testing.T) {
	name := "TestUniversalBo_SetTagVersion"
	_id := "id"
	_tagVersion := uint64(1357)
	ubo := NewUniversalBo(_id, _tagVersion)
	ubo.SetTagVersion(1234)
	if ubo.GetTagVersion() != uint64(1234) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, 1234, ubo.GetTagVersion())
	}
}

func TestUniversalBo_GetTimeCreated_rounding(t *testing.T) {
	name := "TestUniversalBo_GetTimeCreated_rounding"
	roundingOptList := []TimestampRoundSetting{TimestampRoundSettingNone, TimestampRoundSettingNanosecond, TimestampRoundSettingMicrosecond, TimestampRoundSettingMillisecond, TimestampRoundSettingSecond}
	expectedDeltaLimit1List := []int{0, -0, -999, -999_999, -999_999_999}
	expectedDeltaLimit2List := []int{999, 999, 999, 999_999, 999_999}
	for i, roundingOpt := range roundingOptList {
		t.Run(fmt.Sprintf("%v", roundingOpt), func(t *testing.T) {
			now := time.Now()
			_id := "id"
			_tagVersion := uint64(1357)
			ubo := NewUniversalBo(_id, _tagVersion, UboOpt{TimestampRounding: roundingOpt})
			if d := ubo.GetTimeCreated().Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: expected delta between {%v - %v} but received %v", name, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], d)
			}
		})
	}
}

func TestUniversalBo_SetTimeUpdated_rounding(t *testing.T) {
	name := "TestUniversalBo_SetTimeUpdated_rounding"
	roundingOptList := []TimestampRoundSetting{TimestampRoundSettingNone, TimestampRoundSettingNanosecond, TimestampRoundSettingMicrosecond, TimestampRoundSettingMillisecond, TimestampRoundSettingSecond}
	expectedDeltaLimit1List := []int{0, -0, -999, -999_999, -999_999_999}
	expectedDeltaLimit2List := []int{999, 999, 999, 999_999, 999_999}
	for i, roundingOpt := range roundingOptList {
		t.Run(fmt.Sprintf("%v", roundingOpt), func(t *testing.T) {
			_id := "id"
			_tagVersion := uint64(1357)
			ubo := NewUniversalBo(_id, _tagVersion, UboOpt{TimestampRounding: roundingOpt})
			tupdate := time.Now().Add(5 * time.Minute)
			ubo.SetTimeUpdated(tupdate)
			if d := ubo.GetTimeUpdated().Nanosecond() - tupdate.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: expected delta between {%v - %v} but received %v", name, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], d)
			}
		})
	}
}

func TestUniversalBo_SetGet_rounding(t *testing.T) {
	name := "TestUniversalBo_SetGet_rounding"
	roundingOptList := []TimestampRoundSetting{TimestampRoundSettingNone, TimestampRoundSettingNanosecond, TimestampRoundSettingMicrosecond, TimestampRoundSettingMillisecond, TimestampRoundSettingSecond}
	expectedDeltaLimit1List := []int{0, -0, -999, -999_999, -999_999_999}
	expectedDeltaLimit2List := []int{999, 999, 999, 999_999, 999_999}
	for i, roundingOpt := range roundingOptList {
		t.Run(fmt.Sprintf("%v", roundingOpt), func(t *testing.T) {
			_id := "id"
			_tagVersion := uint64(1357)
			ubo := NewUniversalBo(_id, _tagVersion)

			ubo.SetTimestampRounding(roundingOpt)
			if timeRounding := ubo.GetTimestampRounding(); timeRounding != roundingOpt {
				t.Fatalf("%s failed: expected time-rounding %#v but received %#v", name, roundingOpt, timeRounding)
			}

			t1 := time.Now()
			ubo.SetDataAttr("t1", t1)
			ubo.SetExtraAttr("t1", t1)
			v1 := ubo.GetDataAttrAsTimeWithLayoutUnsafe("t1", DefaultTimeLayout)
			if d := v1.Nanosecond() - t1.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", name, t1, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v1, d)
			}
			v1 = ubo.GetExtraAttrAsTimeWithLayoutUnsafe("t1", DefaultTimeLayout)
			if d := v1.Nanosecond() - t1.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", name, t1, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v1, d)
			}

			t2 := t1.Add(102 * time.Second)
			ubo.SetDataAttr("t2", t2)
			ubo.SetExtraAttr("t2", t2)
			v2 := ubo.GetDataAttrAsTimeWithLayoutUnsafe("t2", DefaultTimeLayout)
			if d := v2.Nanosecond() - t2.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", name, t2, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v2, d)
			}
			v2 = ubo.GetExtraAttrAsTimeWithLayoutUnsafe("t2", DefaultTimeLayout)
			if d := v2.Nanosecond() - t2.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", name, t2, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v2, d)
			}
		})
	}
}

func TestUniversalBo_SetExtraAttr(t *testing.T) {
	testName := "TestUniversalBo_SetExtraAttr"
	_id := "id"
	_tagVersion := uint64(1357)
	fieldNameList := []string{"str", "int", "uint", "float", "bool"}
	fieldTypeList := []reflect.Type{reddo.TypeString, reddo.TypeInt, reddo.TypeUint, reddo.TypeFloat, reddo.TypeBool}
	fieldValueList := []interface{}{"a string", int64(123), uint64(456), float64(12.56), true}
	ubo := NewUniversalBo(_id, _tagVersion)
	for i, f := range fieldNameList {
		ubo.SetExtraAttr(f, fieldValueList[i])
	}

	m := ubo.GetExtraAttrs()
	for i, f := range fieldNameList {
		if v, ok := m[f]; !ok {
			t.Fatalf("%s failed: field %s does not exist", testName, f)
		} else if v != fieldValueList[i] {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, fieldValueList[i], v)
		}

		if v := ubo.GetExtraAttr(f); v == nil {
			t.Fatalf("%s failed: field %s does not exist", testName, f)
		} else if v != fieldValueList[i] {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, fieldValueList[i], v)
		}

		if v, err := ubo.GetExtraAttrAs(f, fieldTypeList[i]); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if v != fieldValueList[i] {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, fieldValueList[i], v)
		}

		if v := ubo.GetExtraAttrAsUnsafe(f, fieldTypeList[i]); v == nil {
			t.Fatalf("%s failed: field %s does not exist", testName, f)
		} else if v != fieldValueList[i] {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, fieldValueList[i], v)
		}

		delete(m, f)
	}
	if len(m) > 0 {
		t.Fatalf("%s failed: there are unexpected extra fields %#v", testName, m)
	}
}

func TestUniversalBo_SetExtraAttr_time(t *testing.T) {
	testName := "TestUniversalBo_SetExtraAttr_time"
	roundingOptList := []TimestampRoundSetting{TimestampRoundSettingNone, TimestampRoundSettingNanosecond, TimestampRoundSettingMicrosecond, TimestampRoundSettingMillisecond, TimestampRoundSettingSecond}
	expectedDeltaLimit1List := []int{0, -0, -999, -999_999, -999_999_999}
	expectedDeltaLimit2List := []int{999, 999, 999, 999_999, 999_999}
	for i, roundingOpt := range roundingOptList {
		t.Run(fmt.Sprintf("%v", roundingOpt), func(t *testing.T) {
			_id := "id"
			_tagVersion := uint64(1357)
			ubo := NewUniversalBo(_id, _tagVersion, UboOpt{TimestampRounding: roundingOpt})
			now := time.Now()
			ubo.SetExtraAttr("tstr", now.Format(time.RFC3339Nano))
			ubo.SetExtraAttr("t", now)
			ubo.SetExtraAttr("tp", &now)

			if v, err := ubo.GetExtraAttrAsTimeWithLayout("tstr", DefaultTimeLayout); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}

			if v, err := ubo.GetExtraAttrAsTimeWithLayout("t", DefaultTimeLayout); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}

			if v, err := ubo.GetExtraAttrAsTimeWithLayout("tp", DefaultTimeLayout); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}

			v := ubo.GetExtraAttrAsTimeWithLayoutUnsafe("tstr", DefaultTimeLayout)
			if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}

			v = ubo.GetExtraAttrAsTimeWithLayoutUnsafe("t", DefaultTimeLayout)
			if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}

			v = ubo.GetExtraAttrAsTimeWithLayoutUnsafe("tp", DefaultTimeLayout)
			if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}
		})
	}
}

func TestUniversalBo_SetDataAttr(t *testing.T) {
	testName := "TestUniversalBo_SetDataAttr"
	_id := "id"
	_tagVersion := uint64(1357)
	fieldNameList := []string{"str", "v[0].int", "v[1].uint", "r.e.a.l.float", "v[0].bool"}
	fieldTypeList := []reflect.Type{reddo.TypeString, reddo.TypeInt, reddo.TypeUint, reddo.TypeFloat, reddo.TypeBool}
	fieldValueList := []interface{}{"a string", int64(123), uint64(456), float64(12.56), true}
	ubo := NewUniversalBo(_id, _tagVersion)
	for i, f := range fieldNameList {
		ubo.SetDataAttr(f, fieldValueList[i])
	}

	for i, f := range fieldNameList {
		if v, err := ubo.GetDataAttr(f); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if v != fieldValueList[i] {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, fieldValueList[i], v)
		}

		if v := ubo.GetDataAttrUnsafe(f); v == nil {
			t.Fatalf("%s failed: field %s does not exist", testName, f)
		} else if v != fieldValueList[i] {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, fieldValueList[i], v)
		}

		if v, err := ubo.GetDataAttrAs(f, fieldTypeList[i]); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		} else if v != fieldValueList[i] {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, fieldValueList[i], v)
		}

		if v := ubo.GetDataAttrAsUnsafe(f, fieldTypeList[i]); v == nil {
			t.Fatalf("%s failed: field %s does not exist", testName, f)
		} else if v != fieldValueList[i] {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, fieldValueList[i], v)
		}
	}
}

func TestUniversalBo_SetDataAttr_time(t *testing.T) {
	testName := "TestUniversalBo_SetDataAttr_time"
	roundingOptList := []TimestampRoundSetting{TimestampRoundSettingNone, TimestampRoundSettingNanosecond, TimestampRoundSettingMicrosecond, TimestampRoundSettingMillisecond, TimestampRoundSettingSecond}
	expectedDeltaLimit1List := []int{0, -0, -999, -999_999, -999_999_999}
	expectedDeltaLimit2List := []int{999, 999, 999, 999_999, 999_999}
	for i, roundingOpt := range roundingOptList {
		t.Run(fmt.Sprintf("%v", roundingOpt), func(t *testing.T) {
			_id := "id"
			_tagVersion := uint64(1357)
			ubo := NewUniversalBo(_id, _tagVersion, UboOpt{TimestampRounding: roundingOpt})
			now := time.Now()
			ubo.SetDataAttr("tstr", now.Format(time.RFC3339Nano))
			ubo.SetDataAttr("t.inner[0]", now)
			ubo.SetDataAttr("t.inner[1]", &now)

			if v, err := ubo.GetDataAttrAsTimeWithLayout("tstr", DefaultTimeLayout); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}

			if v, err := ubo.GetDataAttrAsTimeWithLayout("t.inner[0]", DefaultTimeLayout); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}

			if v, err := ubo.GetDataAttrAsTimeWithLayout("t.inner[1]", DefaultTimeLayout); err != nil {
				t.Fatalf("%s failed: %s", testName, err)
			} else if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}

			v := ubo.GetDataAttrAsTimeWithLayoutUnsafe("tstr", DefaultTimeLayout)
			if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}

			v = ubo.GetDataAttrAsTimeWithLayoutUnsafe("t.inner[0]", DefaultTimeLayout)
			if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}

			v = ubo.GetDataAttrAsTimeWithLayoutUnsafe("t.inner[1]", DefaultTimeLayout)
			if d := v.Nanosecond() - now.Nanosecond(); d > expectedDeltaLimit2List[i] || d < expectedDeltaLimit1List[i] {
				t.Fatalf("%s failed: original time %s - expected delta between {%v - %v} / stored time %s - delta %v", testName, now, expectedDeltaLimit1List[i], expectedDeltaLimit2List[i], v, d)
			}
		})
	}
}

func TestUniversalBo_Checksum(t *testing.T) {
	name := "TestUniversalBo_Checksum"
	_id := "id"
	_tagVersion := uint64(1357)
	ubo := NewUniversalBo(_id, _tagVersion)
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
	_id := "id"
	_tagVersion := uint64(1357)
	ubo := NewUniversalBo(_id, _tagVersion)
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
	ubo := NewUniversalBoFromGbo(gbo, UboOpt{TimestampRounding: TimestampRoundSettingNone})
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
