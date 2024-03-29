// Package henge is an out-of-the-box NoSQL style universal data access layer implementation.
//
// See project's wiki for documentation: https://github.com/btnguyen2k/henge/wiki
package henge

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/btnguyen2k/consu/checksum"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/consu/semita"
	"github.com/btnguyen2k/godal"
)

const (
	// Version of package henge.
	Version = "0.6.0"
)

/*----------------------------------------------------------------------*/

// clone a map, deep clone if possible.
func cloneMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	result := make(map[string]interface{})
	for k, v := range src {
		switch v.(type) {
		case []interface{}:
			result[k] = cloneSlice(v.([]interface{}))
		case *[]interface{}:
			temp := cloneSlice(*v.(*[]interface{}))
			result[k] = &temp
		case map[string]interface{}:
			result[k] = cloneMap(v.(map[string]interface{}))
		case *map[string]interface{}:
			temp := cloneMap(*v.(*map[string]interface{}))
			result[k] = &temp
		default:
			result[k] = v
		}
	}
	return result
}

// clone a slice, deep clone if possible.
func cloneSlice(src []interface{}) []interface{} {
	if src == nil {
		return nil
	}
	result := make([]interface{}, len(src))
	for i, v := range src {
		switch v.(type) {
		case []interface{}:
			result[i] = cloneSlice(v.([]interface{}))
		case *[]interface{}:
			temp := cloneSlice(*v.(*[]interface{}))
			result[i] = &temp
		case map[string]interface{}:
			result[i] = cloneMap(v.(map[string]interface{}))
		case *map[string]interface{}:
			temp := cloneMap(*v.(*map[string]interface{}))
			result[i] = &temp
		default:
			result[i] = v
		}
	}
	return result
}

// UboOpt can be supplied to NewUniversalBo and NewUniversalBoFromGbo to specify newly created BO's settings.
//
// Available since v0.5.4
type UboOpt struct {
	TimeLayout        string
	TimestampRounding TimestampRoundingSetting
}

func _extractTimeLayout(opts ...UboOpt) string {
	for _, opt := range opts {
		if opt.TimeLayout != "" {
			return opt.TimeLayout
		}
	}
	return DefaultTimeLayout
}

func _extractTimestampRounding(opts ...UboOpt) TimestampRoundingSetting {
	for _, opt := range opts {
		if opt.TimestampRounding >= TimestampRoundingSettingNone && opt.TimestampRounding <= TimestampRoundingSettingSecond {
			return opt.TimestampRounding
		}
	}
	return DefaultTimestampRoundingSetting
}

// NewUniversalBo is helper function to create a new UniversalBo instance.
//
// Note: id will be space-trimmed.
func NewUniversalBo(id string, tagVersion uint64, opts ...UboOpt) *UniversalBo {
	now := time.Now()
	bo := &UniversalBo{
		id:                 strings.TrimSpace(id),
		timeCreated:        now,
		timeUpdated:        now,
		tagVersion:         tagVersion,
		_dirty:             true,
		_extraAttrs:        make(map[string]interface{}),
		_timestampRounding: _extractTimestampRounding(opts...),
	}
	return bo.Sync(UboSyncOpts{UpdateTimestampIfChecksumChange: true})
}

// NewUniversalBoFromGbo is helper function to construct a new UniversalBo from transforms godal.IGenericBo.
//
// Available since v0.4.1
func NewUniversalBoFromGbo(gbo godal.IGenericBo, opts ...UboOpt) *UniversalBo {
	if gbo == nil {
		return nil
	}
	extraAttrs := make(map[string]interface{})
	gbo.GboTransferViaJson(&extraAttrs)
	for _, field := range topLevelFieldList {
		delete(extraAttrs, field)
	}
	_timeLayout := _extractTimeLayout(opts...)
	_timestampRounding := _extractTimestampRounding(opts...)
	tcreated, _ := gbo.GboGetTimeWithLayout(FieldTimeCreated, _timeLayout)
	tupdated, _ := gbo.GboGetTimeWithLayout(FieldTimeUpdated, _timeLayout)
	bo := &UniversalBo{
		id:                 gbo.GboGetAttrUnsafe(FieldId, reddo.TypeString).(string),
		dataJson:           gbo.GboGetAttrUnsafe(FieldData, reddo.TypeString).(string),
		checksum:           gbo.GboGetAttrUnsafe(FieldChecksum, reddo.TypeString).(string),
		timeCreated:        tcreated,
		timeUpdated:        tupdated,
		tagVersion:         gbo.GboGetAttrUnsafe(FieldTagVersion, reddo.TypeUint).(uint64),
		_extraAttrs:        extraAttrs,
		_dirty:             true,
		_timestampRounding: _timestampRounding,
	}
	if err := bo._parseDataJson(dataInitNone); err != nil {
		return nil
	}
	return bo._sync()
}

const (
	// FieldId is a top level field: BO's unique id.
	FieldId = "id"

	// FieldData is a top level field: BO's user-defined attributes in JSON format.
	FieldData = "data"

	// FieldTagVersion is a top level field: BO's "tag-version" - a value that can be used for compatibility check or data migration.
	FieldTagVersion = "tver"

	// FieldChecksum is a top level field: checksum of BO's value.
	FieldChecksum = "csum"

	// FieldTimeCreated is a top level field: BO's creation timestamp.
	FieldTimeCreated = "tcre"

	// FieldTimeUpdated is a top level field: BO's last-updated timestamp.
	FieldTimeUpdated = "tupd"

	// FieldExtras is an internally used field.
	FieldExtras = "_ext"
)

// TimestampRoundingSetting specifies how UniversalBo would round timestamp before storing.
//
// Available since v0.4.0 (renamed to TimestampRoundingSetting since v0.5.6)
type TimestampRoundingSetting int

const (
	// TimestampRoundingSettingNone specifies that timestamp is not rounded.
	TimestampRoundingSettingNone TimestampRoundingSetting = iota

	// TimestampRoundingSettingNanosecond specifies that timestamp is rounded to nanosecond.
	TimestampRoundingSettingNanosecond

	// TimestampRoundingSettingMicrosecond specifies that timestamp is rounded to microsecond.
	TimestampRoundingSettingMicrosecond

	// TimestampRoundingSettingMillisecond specifies that timestamp is rounded to millisecond.
	TimestampRoundingSettingMillisecond

	// TimestampRoundingSettingSecond specifies that timestamp is rounded to second.
	TimestampRoundingSettingSecond
)

const (
	// DefaultTimestampRoundingSetting is the default TimestampRoundingSetting to be used if such setting is not specified.
	//
	// Available since v0.5.4
	DefaultTimestampRoundingSetting = TimestampRoundingSettingSecond

	// DefaultTimeLayout is the default layout used by UniversalBo to convert datetime values to string and vice versa if such setting is not specified.
	//
	// Available since v0.5.4
	DefaultTimeLayout = time.RFC3339Nano
)

var (
	topLevelFieldList = []string{FieldId, FieldData, FieldChecksum, FieldTagVersion, FieldTimeCreated, FieldTimeUpdated}
)

// UboSyncOpts specifies behaviors of UniversalBo.Sync function.
//
// Available since v0.5.3
type UboSyncOpts struct {
	UpdateTimestamp                 bool // if true, UniversalBo's last-updated timestamp is updated with the current timestamp
	UpdateTimestampIfChecksumChange bool // if true, UniversalBo's last-updated timestamp is updated with the current timestamp if BO's checksum changes
}

// UniversalBo is the NoSQL style universal business object. Business attributes are stored in a JSON-encoded attribute.
type UniversalBo struct {
	/* top level attributes */
	id          string    `json:"id"`   // bo's unique identifier
	dataJson    string    `json:"data"` // bo's attributes encoded as JSON string
	tagVersion  uint64    `json:"tver"` // for application internal use (can be used for compatibility check or data migration)
	checksum    string    `json:"csum"` // bo's checksum (should not take update-time into account)
	timeCreated time.Time `json:"tcre"` // bo's creation timestamp
	timeUpdated time.Time `json:"tupd"` // bo's last-updated timestamp

	/* computed attributes */
	_data  interface{}    `json:"-"` // deserialized form of data-json
	_sdata *semita.Semita `json:"-"` // used to access data in hierarchy manner

	/* internal attributes */
	_extraAttrs        map[string]interface{} `json:"_ext"` // other top-level arbitrary attributes
	_lock              sync.RWMutex
	_dirty             bool
	_timestampRounding TimestampRoundingSetting
}

// FuncPreUboToMap is used by UniversalBo.ToMap to export UniversalBo's attributes to a map[string]interface{}.
type FuncPreUboToMap func(*UniversalBo) map[string]interface{}

// FuncPostUboToMap is used by UniversalBo.ToMap to transform the result map (output from FuncPreUboToMap) to another map[string]interface{}.
type FuncPostUboToMap func(map[string]interface{}) map[string]interface{}

// DefaultFuncPreUboToMap is default implementation of FuncPreUboToMap.
//
// This function exports the input UniversalBo as-is to a map with following fields:
// { FieldId (string), FieldData (string), FieldTagVersion (uint64), FieldChecksum (string),
// FieldTimeCreated (time.Time), FieldTimeUpdated (time.Time), FieldExtras (map[string]interface{}) }
var DefaultFuncPreUboToMap FuncPreUboToMap = func(_ubo *UniversalBo) map[string]interface{} {
	ubo := _ubo.Clone()
	return map[string]interface{}{
		FieldId:          ubo.id,
		FieldData:        ubo.dataJson,
		FieldTagVersion:  ubo.tagVersion,
		FieldChecksum:    ubo.checksum,
		FieldTimeCreated: ubo.timeCreated,
		FieldTimeUpdated: ubo.timeUpdated,
		FieldExtras:      cloneMap(ubo._extraAttrs),
	}
}

// ToGenericBo exports the BO data to a godal.IGenericBo.
//   - the exported godal.IGenericBo is populated with fields FieldId, FieldData, FieldChecksum, FieldTimeCreated, FieldTimeUpdated and FieldTagVersion.
//
// Available since v0.4.1
func (ubo *UniversalBo) ToGenericBo() godal.IGenericBo {
	clone := ubo.Clone()
	gbo := godal.NewGenericBo()
	gbo.GboSetAttr(FieldId, clone.id)
	gbo.GboSetAttr(FieldData, clone.dataJson)
	gbo.GboSetAttr(FieldChecksum, clone.checksum)
	gbo.GboSetAttr(FieldTimeCreated, clone.timeCreated)
	gbo.GboSetAttr(FieldTimeUpdated, clone.timeUpdated)
	gbo.GboSetAttr(FieldTagVersion, clone.tagVersion)
	for k, v := range clone._extraAttrs {
		gbo.GboSetAttr(k, v)
	}
	return gbo
}

// ToMap exports the BO data to a map[string]interface{}.
//   - preFunc is used to export BO data to a map. If not supplied, DefaultFuncPreUboToMap is used.
//   - postFunc is used to transform the result map (output from preFunc) to the final result. If not supplied, the result from preFunc is returned as-is.
func (ubo *UniversalBo) ToMap(preFunc FuncPreUboToMap, postFunc FuncPostUboToMap) map[string]interface{} {
	if preFunc == nil {
		preFunc = DefaultFuncPreUboToMap
	}
	result := preFunc(ubo.Clone())
	if postFunc != nil {
		result = postFunc(result)
	}
	return result
}

// MarshalJSON implements json.encode.Marshaler.MarshalJSON.
func (ubo *UniversalBo) MarshalJSON() ([]byte, error) {
	ubo.Sync(UboSyncOpts{UpdateTimestampIfChecksumChange: true})
	ubo._lock.RLock()
	defer ubo._lock.RUnlock()
	m := map[string]interface{}{
		FieldId:          ubo.id,
		FieldData:        ubo.dataJson,
		FieldTagVersion:  ubo.tagVersion,
		FieldChecksum:    ubo.checksum,
		FieldTimeCreated: ubo.timeCreated.Format(DefaultTimeLayout),
		FieldTimeUpdated: ubo.timeUpdated.Format(DefaultTimeLayout),
		FieldExtras:      cloneMap(ubo._extraAttrs),
	}
	return json.Marshal(m)
}

// UnmarshalJSON implements json.decode.Unmarshaler.UnmarshalJSON.
func (ubo *UniversalBo) UnmarshalJSON(data []byte) error {
	var m map[string]interface{}
	err := json.Unmarshal(data, &m)
	if err == nil {
		m[FieldId], err = reddo.ToString(m[FieldId])
	}
	if err == nil {
		m[FieldData], err = reddo.ToString(m[FieldData])
	}
	if err == nil {
		m[FieldTagVersion], err = reddo.ToUint(m[FieldTagVersion])
	}
	if err == nil {
		m[FieldChecksum], err = reddo.ToString(m[FieldChecksum])
	}
	if err == nil {
		m[FieldTimeCreated], err = reddo.ToTimeWithLayout(m[FieldTimeCreated], time.RFC3339Nano)
	}
	if err == nil {
		m[FieldTimeUpdated], err = reddo.ToTimeWithLayout(m[FieldTimeUpdated], time.RFC3339Nano)
	}
	if err == nil {
		m[FieldExtras], err = reddo.ToMap(m[FieldExtras], reflect.TypeOf(map[string]interface{}{}))
	}
	if err != nil {
		return err
	}

	ubo._lock.Lock()
	defer ubo._lock.Unlock()
	ubo.id = m[FieldId].(string)
	ubo.tagVersion = m[FieldTagVersion].(uint64)
	ubo.checksum = m[FieldChecksum].(string)
	ubo.timeCreated = m[FieldTimeCreated].(time.Time)
	ubo.timeUpdated = m[FieldTimeUpdated].(time.Time)
	ubo._extraAttrs = make(map[string]interface{})
	if m[FieldExtras] != nil {
		ubo._extraAttrs = m[FieldExtras].(map[string]interface{})
	}
	ubo._setDataJson(m[FieldData].(string))
	ubo._sync(UboSyncOpts{UpdateTimestampIfChecksumChange: true})
	return nil
}

// GetId returns value of bo's 'id' field.
func (ubo *UniversalBo) GetId() string {
	return ubo.id
}

// SetId sets value of bo's 'id' field.
func (ubo *UniversalBo) SetId(value string) *UniversalBo {
	ubo._lock.Lock()
	defer ubo._lock.Unlock()
	ubo.id = strings.TrimSpace(value)
	ubo._dirty = true
	return ubo
}

// GetDataJson returns bo's user-defined attributes in JSON format.
func (ubo *UniversalBo) GetDataJson() string {
	return ubo.dataJson
}

type dataInitType int

const (
	dataInitNone dataInitType = iota
	dataInitMap
	dataInitSlice
)

var (
	errorDataInitedAsMap   = errors.New("data is initialized as empty map")
	errorDataInitedAsSlice = errors.New("data is initialized as empty slice")
)

func (ubo *UniversalBo) _parseDataJson(dataInit dataInitType) error {
	err := json.Unmarshal([]byte(ubo.dataJson), &ubo._data)
	if err != nil || ubo._data == nil {
		if dataInit == dataInitMap {
			ubo._data = make(map[string]interface{})
			err = errorDataInitedAsMap
		} else if dataInit == dataInitSlice {
			ubo._data = make([]interface{}, 0)
			err = errorDataInitedAsSlice
		} else if dataInit != dataInitNone {
			ubo._data = nil
		}
	}
	if ubo._data != nil {
		ubo._sdata = semita.NewSemita(&ubo._data)
	} else {
		ubo._sdata = nil
	}
	return err
}

func (ubo *UniversalBo) _setDataJson(value string) *UniversalBo {
	ubo.dataJson = strings.TrimSpace(value)
	ubo._parseDataJson(dataInitNone)
	ubo._dirty = true
	return ubo
}

// SetDataJson sets bo's user-defined attributes as a whole in JSON format.
func (ubo *UniversalBo) SetDataJson(value string) *UniversalBo {
	ubo._lock.Lock()
	defer ubo._lock.Unlock()
	return ubo._setDataJson(value)
}

// GetTagVersion returns value of bo's 'tag-version' field.
func (ubo *UniversalBo) GetTagVersion() uint64 {
	return ubo.tagVersion
}

// SetTagVersion sets value of bo's 'tag-version' field.
func (ubo *UniversalBo) SetTagVersion(value uint64) *UniversalBo {
	ubo._lock.Lock()
	defer ubo._lock.Unlock()
	ubo.tagVersion = value
	ubo._dirty = true
	return ubo
}

// GetChecksum returns value of bo's 'checksum' field.
func (ubo *UniversalBo) GetChecksum() string {
	return ubo.checksum
}

// RoundTimestamp rounds the input time according to bo's timestamp-rounding setting and returns the result.
//
// Available since v0.5.6
func (ubo *UniversalBo) RoundTimestamp(t time.Time) time.Time {
	switch ubo._timestampRounding {
	case TimestampRoundingSettingMicrosecond:
		return t.Round(time.Microsecond)
	case TimestampRoundingSettingMillisecond:
		return t.Round(time.Millisecond)
	case TimestampRoundingSettingSecond:
		return t.Round(time.Second)
	}
	return t
}

// NormalizeTimestampForStoring normalizes the input time for storing.
//   - Firstly, the input time is rounded according to bo's timestamp-rounding setting.
//   - Then, the rounded time is converted to string according to the specified layout.
//
// Available since v0.5.6
func (ubo *UniversalBo) NormalizeTimestampForStoring(t time.Time, layout string) string {
	return ubo.RoundTimestamp(t).Format(layout)
}

// GetTimestampRounding returns this BO's timestamp-rounding setting.
// Timestamp-rounding controls how BO's create/update timestamp will be rounded before storing.
//
// Available since v0.5.4
func (ubo *UniversalBo) GetTimestampRounding() TimestampRoundingSetting {
	return ubo._timestampRounding
}

// SetTimestampRounding updates this BO's timestamp-rounding setting.
// Timestamp-rounding controls how BO's create/update timestamp will be rounded before storing.
//
// Available since v0.5.4
func (ubo *UniversalBo) SetTimestampRounding(value TimestampRoundingSetting) *UniversalBo {
	ubo._timestampRounding = value
	return ubo
}

// GetTimeCreated returns value of bo's 'timestamp-created' field.
func (ubo *UniversalBo) GetTimeCreated() time.Time {
	return ubo.timeCreated
}

// GetTimeUpdated returns value of bo's 'timestamp-updated' field.
func (ubo *UniversalBo) GetTimeUpdated() time.Time {
	return ubo.timeUpdated
}

// SetTimeUpdated sets value of bo's 'timestamp-updated' field.
func (ubo *UniversalBo) SetTimeUpdated(value time.Time) *UniversalBo {
	ubo.timeUpdated = value
	return ubo
}

// IsDirty returns 'true' if bo's data has been modified.
func (ubo *UniversalBo) IsDirty() bool {
	return ubo._dirty
}

// GetDataAttr is alias of GetDataAttrAs(path, nil).
func (ubo *UniversalBo) GetDataAttr(path string) (interface{}, error) {
	return ubo.GetDataAttrAs(path, nil)
}

// GetDataAttrUnsafe is similar to GetDataAttr but ignoring error.
func (ubo *UniversalBo) GetDataAttrUnsafe(path string) interface{} {
	return ubo.GetDataAttrAsUnsafe(path, nil)
}

// GetDataAttrAsUnsafe is similar to GetDataAttrAs but ignoring error.
func (ubo *UniversalBo) GetDataAttrAsUnsafe(path string, typ reflect.Type) interface{} {
	v, _ := ubo.GetDataAttrAs(path, typ)
	return v
}

// GetDataAttrAsTimeWithLayout returns value, converted to time, of a data attribute located at 'path'.
func (ubo *UniversalBo) GetDataAttrAsTimeWithLayout(path, layout string) (time.Time, error) {
	v, _ := ubo.GetDataAttr(path)
	return reddo.ToTimeWithLayout(v, layout)
}

// GetDataAttrAsTimeWithLayoutUnsafe is similar to GetDataAttrAsTimeWithLayout but ignoring error.
func (ubo *UniversalBo) GetDataAttrAsTimeWithLayoutUnsafe(path, layout string) time.Time {
	t, _ := ubo.GetDataAttrAsTimeWithLayout(path, layout)
	return t
}

func (ubo *UniversalBo) _initSdata(path string) {
	if ubo._sdata == nil {
		dataInit := dataInitMap
		if strings.HasSuffix(path, "[") {
			dataInit = dataInitSlice
		}
		ubo._parseDataJson(dataInit)
	}
}

// GetDataAttrAs returns value, converted to the specified type, of a data attribute located at 'path'.
func (ubo *UniversalBo) GetDataAttrAs(path string, typ reflect.Type) (interface{}, error) {
	ubo._lock.RLock()
	defer ubo._lock.RUnlock()
	ubo._initSdata(path)
	if ubo._sdata == nil {
		return nil, errors.New("cannot get data at path [" + path + "]")
	}
	return ubo._sdata.GetValueOfType(path, typ)
}

// SetDataAttr sets value of a data attribute located at 'path'.
//
// - If value is a time.Time (or *time.Time), the time value is rounded according to the BO's timestamp-rounding setting.
// - (since v0.5.5) Furthermore, the time value is converted to string (using layout DefaultTimeLayout) before storing.
func (ubo *UniversalBo) SetDataAttr(path string, value interface{}) error {
	ubo._lock.Lock()
	defer ubo._lock.Unlock()
	ubo._dirty = true
	ubo._initSdata(path)
	if ubo._sdata == nil {
		return errors.New("cannot set data at path [" + path + "]")
	}
	switch value.(type) {
	case time.Time:
		value = ubo.NormalizeTimestampForStoring(value.(time.Time), DefaultTimeLayout)
	case *time.Time:
		value = ubo.NormalizeTimestampForStoring(*value.(*time.Time), DefaultTimeLayout)
	}
	return ubo._sdata.SetValue(path, value)
}

// GetExtraAttrs returns the 'extra-attrs' map.
func (ubo *UniversalBo) GetExtraAttrs() map[string]interface{} {
	ubo._lock.RLock()
	defer ubo._lock.RUnlock()
	return cloneMap(ubo._extraAttrs)
}

// GetExtraAttr returns value of an 'extra' attribute specified by 'key'.
func (ubo *UniversalBo) GetExtraAttr(key string) interface{} {
	v := ubo._extraAttrs[key]
	return v
}

// GetExtraAttrAs returns value, converted to the specified type, of an 'extra' attribute specified by 'key'.
func (ubo *UniversalBo) GetExtraAttrAs(key string, typ reflect.Type) (interface{}, error) {
	v := ubo.GetExtraAttr(key)
	return reddo.Convert(v, typ)
}

// GetExtraAttrAsTimeWithLayout returns value, converted to time, of an 'extra' attribute specified by 'key'.
func (ubo *UniversalBo) GetExtraAttrAsTimeWithLayout(key, layout string) (time.Time, error) {
	v := ubo.GetExtraAttr(key)
	return reddo.ToTimeWithLayout(v, layout)
}

// GetExtraAttrAsUnsafe is similar to GetExtraAttrAs but no error is returned.
func (ubo *UniversalBo) GetExtraAttrAsUnsafe(key string, typ reflect.Type) interface{} {
	v, _ := ubo.GetExtraAttrAs(key, typ)
	return v
}

// GetExtraAttrAsTimeWithLayoutUnsafe is similar to GetExtraAttrAsTimeWithLayout but no error is returned.
func (ubo *UniversalBo) GetExtraAttrAsTimeWithLayoutUnsafe(key, layout string) time.Time {
	t, _ := ubo.GetExtraAttrAsTimeWithLayout(key, layout)
	return t
}

// SetExtraAttr sets value of an 'extra' attribute specified by 'key'.
//
// - If value is a time.Time (or *time.Time), the time value is rounded according to the BO's timestamp-rounding setting.
func (ubo *UniversalBo) SetExtraAttr(key string, value interface{}) *UniversalBo {
	ubo._lock.Lock()
	defer ubo._lock.Unlock()
	if ubo._extraAttrs == nil {
		ubo._extraAttrs = make(map[string]interface{})
	}
	ubo._dirty = true
	switch value.(type) {
	case time.Time:
		value = ubo.RoundTimestamp(value.(time.Time))
	case *time.Time:
		value = ubo.RoundTimestamp(*value.(*time.Time))
	}
	ubo._extraAttrs[key] = value
	return ubo
}

func _requireTimeUpdatedSync(opts ...UboSyncOpts) bool {
	for _, opt := range opts {
		if opt.UpdateTimestamp {
			return true
		}
	}
	return false
}

func _requireTimeUpdatedSyncIfChecksumChange(opts ...UboSyncOpts) bool {
	for _, opt := range opts {
		if opt.UpdateTimestampIfChecksumChange {
			return true
		}
	}
	return false
}

func (ubo *UniversalBo) _sync(opts ...UboSyncOpts) *UniversalBo {
	if ubo._dirty {
		ubo.timeCreated = ubo.RoundTimestamp(ubo.timeCreated)
		ubo.timeUpdated = ubo.RoundTimestamp(ubo.timeUpdated)
		oldChecksum := ubo.checksum
		csumMap := map[string]interface{}{
			"id":          ubo.id,
			"app_version": ubo.tagVersion,
			"t_created":   ubo.timeCreated.In(time.UTC).Format(DefaultTimeLayout),
			"data":        ubo._data,
			"extra":       ubo._extraAttrs,
		}
		ubo.checksum = fmt.Sprintf("%x", checksum.Md5Checksum(csumMap))
		if _requireTimeUpdatedSync(opts...) ||
			(_requireTimeUpdatedSyncIfChecksumChange(opts...) && oldChecksum != ubo.checksum) {
			ubo.timeUpdated = ubo.RoundTimestamp(time.Now())
		}
		js, _ := json.Marshal(ubo._data)
		ubo.dataJson = string(js)
		ubo._dirty = false
	}
	return ubo
}

// Sync syncs user-defined attribute values to JSON format.
func (ubo *UniversalBo) Sync(opts ...UboSyncOpts) *UniversalBo {
	ubo._lock.Lock()
	defer ubo._lock.Unlock()
	return ubo._sync(opts...)
}

// Clone creates a cloned copy of the business object.
func (ubo *UniversalBo) Clone() *UniversalBo {
	ubo._lock.RLock()
	defer ubo._lock.RUnlock()
	ubo._sync(UboSyncOpts{UpdateTimestampIfChecksumChange: true})
	clone := &UniversalBo{
		id:                 ubo.id,
		dataJson:           ubo.dataJson,
		tagVersion:         ubo.tagVersion,
		checksum:           ubo.checksum,
		timeCreated:        ubo.timeCreated,
		timeUpdated:        ubo.timeUpdated,
		_data:              nil,
		_sdata:             nil,
		_extraAttrs:        cloneMap(ubo._extraAttrs),
		_dirty:             false,
		_timestampRounding: ubo._timestampRounding,
	}
	clone._parseDataJson(dataInitNone)
	return clone
}

/*----------------------------------------------------------------------*/

// UniversalDao defines API to access UniversalBo storage.
type UniversalDao interface {
	// ToUniversalBo transforms godal.IGenericBo to business object.
	ToUniversalBo(gbo godal.IGenericBo) *UniversalBo

	// ToGenericBo transforms the business object to godal.IGenericBo.
	ToGenericBo(ubo *UniversalBo) godal.IGenericBo

	// Delete removes the specified business object from storage.
	// This function returns true if number of deleted records is non-zero.
	Delete(bo *UniversalBo) (bool, error)

	// Create persists a new business object to storage.
	// This function returns true if number of inserted records is non-zero.
	Create(bo *UniversalBo) (bool, error)

	// Get retrieves a business object from storage.
	Get(id string) (*UniversalBo, error)

	// GetN retrieves N business objects from storage.
	GetN(fromOffset, maxNumRows int, filter godal.FilterOpt, sorting *godal.SortingOpt) ([]*UniversalBo, error)

	// GetAll retrieves all available business objects from storage.
	GetAll(filter godal.FilterOpt, sorting *godal.SortingOpt) ([]*UniversalBo, error)

	// Update modifies an existing business object.
	// This function returns true if number of updated records is non-zero.
	Update(bo *UniversalBo) (bool, error)

	// Save creates new business object or updates an existing one.
	// This function returns the existing record along with value true if number of inserted/updated record is non-zero.
	Save(bo *UniversalBo) (bool, *UniversalBo, error)
}
