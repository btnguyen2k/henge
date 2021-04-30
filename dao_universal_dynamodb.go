package henge

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/btnguyen2k/consu/checksum"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/dynamodb"
	"github.com/btnguyen2k/prom"
)

const (
	// AwsDynamodbUidxTableSuffix holds prefix string of the secondary table which henge uses to manage unique indexes.
	AwsDynamodbUidxTableSuffix = "_uidx"

	// AwsDynamodbUidxTableColName holds name of the secondary table's column to store unique index name.
	AwsDynamodbUidxTableColName = "uname"

	// AwsDynamodbUidxTableColHash holds name of the secondary table's column to store unique index hash value.
	AwsDynamodbUidxTableColHash = "uhash"
)

// toFilterMap translates a godal.FilterOpt to DynamoDB-compatible filter map.
func toFilterMap(filter godal.FilterOpt) (map[string]interface{}, error) {
	if filter == nil {
		return nil, nil
	}
	switch filter.(type) {
	case godal.FilterOptFieldOpValue:
		f := filter.(godal.FilterOptFieldOpValue)
		return toFilterMap(&f)
	case *godal.FilterOptFieldOpValue:
		f := filter.(*godal.FilterOptFieldOpValue)
		if f.Operator != godal.FilterOpEqual {
			return nil, fmt.Errorf("invalid operator \"%#v\", only accept FilterOptFieldOpValue with operator FilterOpEqual", f.Operator)
		}
		return map[string]interface{}{f.FieldName: f.Value}, nil
	case godal.FilterOptFieldIsNull:
		f := filter.(godal.FilterOptFieldIsNull)
		return toFilterMap(&f)
	case *godal.FilterOptFieldIsNull:
		f := filter.(*godal.FilterOptFieldIsNull)
		return map[string]interface{}{f.FieldName: nil}, nil
	case godal.FilterOptAnd:
		f := filter.(godal.FilterOptAnd)
		return toFilterMap(&f)
	case *godal.FilterOptAnd:
		f := filter.(*godal.FilterOptAnd)
		result := make(map[string]interface{})
		for _, inner := range f.Filters {
			innerF, err := toFilterMap(inner)
			if err != nil {
				return nil, err
			}
			for k, v := range innerF {
				result[k] = v
			}
		}
		return result, nil
	}
	return nil, fmt.Errorf("cannot build filter map from %T", filter)
}

// DynamodbTablesSpec holds specification of DynamoDB tables to be created.
//
// Available: since v0.3.2
type DynamodbTablesSpec struct {
	MainTableRcu         int64                         // rcu of the main table
	MainTableWcu         int64                         // wcu of the main table
	MainTableCustomAttrs []prom.AwsDynamodbNameAndType // main table's custom attributes (beside henge's attributes)
	MainTablePkPrefix    string                        // prefix attribute to main table's PK (if defined, PK of the main table is { pkPrefix, FieldId }; otherwise { FieldId } ). MainTablePkPrefix, if specified, must be included in MainTableCustomAttrs.
	CreateUidxTable      bool                          // if true, the secondary table is created
	UidxTableRcu         int64                         // rcu of the secondary table
	UidxTableWcu         int64                         // wcu of the secondary table
}

// InitDynamodbTables initializes a DynamoDB table(s) to store henge business objects.
//   - The main table to store business objects.
//   - The secondary table is used to manage unique indexes. The secondary table has the same base name and suffixed by AwsDynamodbUidxTableSuffix.
//   - The secondary table has the following schema: { AwsDynamodbUidxTableColName, AwsDynamodbUidxTableColHash }
//   - Other than the two tables, no local index or global index is created.
//   - (since v0.3.2) spec.MainTableCustomAttrs can be used to define main table's custom attributes.
//   - (since v0.3.2) spec.MainTablePkPrefix can be used to override main table's PK.
//     - if spec.MainTablePkPrefix is not supplied, main table is created with PK as { FieldId }
//     - otherwise, main table is created with PK as { spec.MainTablePkPrefix, FieldId }
//
// Available: since v0.3.0
func InitDynamodbTables(adc *prom.AwsDynamodbConnect, tableName string, spec *DynamodbTablesSpec) error {
	if spec == nil {
		return errors.New("table spec is nil")
	}
	// main table
	attrDefs := []prom.AwsDynamodbNameAndType{{Name: FieldId, Type: prom.AwsAttrTypeString}}
	if len(spec.MainTableCustomAttrs) > 0 {
		attrDefs = append(attrDefs, spec.MainTableCustomAttrs...)
	}
	pkDefs := []prom.AwsDynamodbNameAndType{{Name: FieldId, Type: prom.AwsKeyTypePartition}}
	if spec.MainTablePkPrefix != "" {
		pkDefs = []prom.AwsDynamodbNameAndType{{Name: spec.MainTablePkPrefix, Type: prom.AwsKeyTypePartition}, {Name: FieldId, Type: prom.AwsKeyTypeSort}}
	}
	err := adc.CreateTable(nil, tableName, spec.MainTableRcu, spec.MainTableWcu, attrDefs, pkDefs)
	if err = prom.AwsIgnoreErrorIfMatched(err, awsdynamodb.ErrCodeTableAlreadyExistsException); err != nil {
		if err = prom.AwsIgnoreErrorIfMatched(err, awsdynamodb.ErrCodeResourceInUseException); err != nil {
			return err
		}
	}

	// secondary table
	if spec.CreateUidxTable {
		attrDefs = []prom.AwsDynamodbNameAndType{
			{Name: AwsDynamodbUidxTableColName, Type: prom.AwsAttrTypeString},
			{Name: AwsDynamodbUidxTableColHash, Type: prom.AwsAttrTypeString},
		}
		pkDefs = []prom.AwsDynamodbNameAndType{
			{Name: AwsDynamodbUidxTableColName, Type: prom.AwsKeyTypePartition},
			{Name: AwsDynamodbUidxTableColHash, Type: prom.AwsKeyTypeSort},
		}
		err = adc.CreateTable(nil, tableName+AwsDynamodbUidxTableSuffix, spec.UidxTableRcu, spec.UidxTableWcu, attrDefs, pkDefs)
		if err = prom.AwsIgnoreErrorIfMatched(err, awsdynamodb.ErrCodeTableAlreadyExistsException); err != nil {
			if err = prom.AwsIgnoreErrorIfMatched(err, awsdynamodb.ErrCodeResourceInUseException); err != nil {
				return err
			}
		}
	}

	return nil
}

// buildRowMapperDynamodb is helper method to build godal.IRowMapper for UniversalDaoDynamodb.
//
// Default partition key is { FieldId }. This can be overridden by pkPrefix. If specified, partition key is { pkPrefix, FieldId }
func buildRowMapperDynamodb(tableName string, pkPrefix string) godal.IRowMapper {
	pkAttrs := []string{FieldId}
	if pkPrefix != "" {
		pkAttrs = []string{pkPrefix, FieldId}
	}
	return &rowMapperDynamodb{wrap: &dynamodb.GenericRowMapperDynamodb{
		ColumnsListMap: map[string][]string{tableName: pkAttrs},
	}}
}

// rowMapperDynamodb is an implementation of godal.IRowMapper specific for AWS DynamoDB.
type rowMapperDynamodb struct {
	wrap godal.IRowMapper
}

func (r *rowMapperDynamodb) ToDbColName(tableName, fieldName string) string {
	return fieldName
}

func (r *rowMapperDynamodb) ToBoFieldName(tableName, colName string) string {
	return colName
}

// ToRow implements godal.IRowMapper.ToRow.
func (r *rowMapperDynamodb) ToRow(tableName string, bo godal.IGenericBo) (interface{}, error) {
	row, err := r.wrap.ToRow(tableName, bo)
	if m, ok := row.(map[string]interface{}); err == nil && ok && m != nil {
		m[FieldTagVersion], _ = bo.GboGetAttr(FieldTagVersion, nil) // tag-version should be integer
		m[FieldTimeCreated], _ = bo.GboGetTimeWithLayout(FieldTimeCreated, TimeLayout)
		m[FieldTimeUpdated], _ = bo.GboGetTimeWithLayout(FieldTimeUpdated, TimeLayout)
		m[FieldData], _ = bo.GboGetAttrUnmarshalJson(FieldData) // Note: FieldData must be JSON-encoded string!
	}
	return row, err
}

// ToBo implements godal.IRowMapper.ToBo.
func (r *rowMapperDynamodb) ToBo(tableName string, row interface{}) (godal.IGenericBo, error) {
	gbo, err := r.wrap.ToBo(tableName, row)
	if err == nil && gbo != nil {
		if data, err := gbo.GboGetAttr(FieldData, nil); err == nil {
			// Note: convert 'data' column from row to JSON-encoded string before storing to FieldData
			if str, ok := data.(string); ok {
				gbo.GboSetAttr(FieldData, str)
			} else if bytes, ok := data.([]byte); ok {
				gbo.GboSetAttr(FieldData, string(bytes))
			} else {
				js, _ := json.Marshal(data)
				gbo.GboSetAttr(FieldData, string(js))
			}
		}
	}
	return gbo, err
}

// ColumnsList implements godal.IRowMapper.ColumnsList.
func (r *rowMapperDynamodb) ColumnsList(tableName string) []string {
	return r.wrap.ColumnsList(tableName)
}

// DynamodbDaoSpec holds specification of UniversalDaoDynamodb to be created.
//
// Available: since v0.3.2
type DynamodbDaoSpec struct {
	PkPrefix      string     // (multi-tenant) if pkPrefix is supplied, table's PK is { PkPrefix, FieldId }, otherwise table's PK is { FieldId }
	PkPrefixValue string     // (multi-tenant) static value for PkPrefix attribute
	UidxAttrs     [][]string // list of unique indexes, each unique index is a combination of table columns
}

// NewUniversalDaoDynamodb is helper method to create UniversalDaoDynamodb instance.
//   - uidxAttrs list of unique indexes, each unique index is a combination of table columns.
//   - the table has default pk as { FieldId }. If pkPrefix is supplied, table pk becomes { pkPrefix, FieldId }.
//   - static value for pkPrefix attribute can be specified via pkPrefixValue.
func NewUniversalDaoDynamodb(adc *prom.AwsDynamodbConnect, tableName string, spec *DynamodbDaoSpec) UniversalDao {
	if spec == nil {
		spec = &DynamodbDaoSpec{}
	}
	dao := &UniversalDaoDynamodb{
		tableName:     tableName,
		pkPrefix:      spec.PkPrefix,
		pkPrefixValue: spec.PkPrefixValue,
		uidxTableName: tableName + AwsDynamodbUidxTableSuffix,
		uidxAttrs:     spec.UidxAttrs,
		uidxHf1:       checksum.Sha1HashFunc,
		uidxHf2:       checksum.Md5HashFunc,
	}
	dao.GenericDaoDynamodb = dynamodb.NewGenericDaoDynamodb(adc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(buildRowMapperDynamodb(tableName, spec.PkPrefix))
	return dao
}

// UniversalDaoDynamodb is AWS DynamoDB-based implementation of UniversalDao.
type UniversalDaoDynamodb struct {
	*dynamodb.GenericDaoDynamodb
	tableName        string            // name of database table to store business objects
	pkPrefix         string            // (since v0.3.2) if pkPrefix is supplied, table has PK as { pkPrefix, FieldId }; otherwise { FieldId }
	pkPrefixValue    string            // (since v0.3.2) static value for pkPrefix attribute
	uidxTableName    string            // name of database table to store unique indexes
	uidxAttrs        [][]string        // list of unique indexes (each unique index is a combination of table columns)
	uidxHf1, uidxHf2 checksum.HashFunc // hash functions used to calculate unique index hash
}

// GetTableName returns name of database table to store business objects.
func (dao *UniversalDaoDynamodb) GetTableName() string {
	return dao.tableName
}

// GetPkPrefix returns value of dao.pkPrefix.
//
// pkPrefix and pkPrefix are used by GdaoCreateFilter.
//
// Available: since v0.3.2
func (dao *UniversalDaoDynamodb) GetPkPrefix() string {
	return dao.pkPrefix
}

// GetPkPrefixValue returns static value for dao.pkPrefix attribute.
//
// pkPrefix and pkPrefix are used by GdaoCreateFilter.
//
// Available: since v0.3.2
func (dao *UniversalDaoDynamodb) GetPkPrefixValue() string {
	return dao.pkPrefixValue
}

// GetUidxTableName returns name of database table to store unique indexes.
func (dao *UniversalDaoDynamodb) GetUidxTableName() string {
	return dao.uidxTableName
}

// GetUidxAttrs returns list of unique indexes (each unique index is a combination of table columns).
func (dao *UniversalDaoDynamodb) GetUidxAttrs() [][]string {
	return dao.uidxAttrs
}

// SetUidxAttrs sets unique indexes (each unique index is a combination of table columns).
func (dao *UniversalDaoDynamodb) SetUidxAttrs(uidxAttrs [][]string) *UniversalDaoDynamodb {
	dao.uidxAttrs = uidxAttrs
	return dao
}

// GetUidxHashFunctions returns the hash functions used to calculate unique index hash.
//
// Currently two hash functions are used.
func (dao *UniversalDaoDynamodb) GetUidxHashFunctions() []checksum.HashFunc {
	return []checksum.HashFunc{dao.uidxHf1, dao.uidxHf2}
}

// SetUidxHashFunctions configures the hash functions used to calculate unique index hash.
//
// Currently two hash functions are used, and two must be different. By default, the following hash functions
// will be used: checksum.Sha1HashFunc and checksum.Md5HashFunc
func (dao *UniversalDaoDynamodb) SetUidxHashFunctions(uidxHashFuncs []checksum.HashFunc) *UniversalDaoDynamodb {
	if len(uidxHashFuncs) > 0 && uidxHashFuncs[0] != nil {
		dao.uidxHf1 = uidxHashFuncs[0]
	} else {
		dao.uidxHf1 = checksum.Sha1HashFunc
	}
	if len(uidxHashFuncs) > 1 && uidxHashFuncs[1] != nil {
		dao.uidxHf2 = uidxHashFuncs[1]
	} else {
		dao.uidxHf2 = checksum.Md5HashFunc
	}
	return dao
}

// BuildUidxValues calculate unique index hash value from a godal.IGenericBo.
//
// The return value is a map {uidxName:uidxHashValue}.
func (dao *UniversalDaoDynamodb) BuildUidxValues(bo godal.IGenericBo) map[string]string {
	if dao.uidxAttrs == nil || len(dao.uidxAttrs) == 0 || bo == nil {
		return nil
	}
	result := make(map[string]string)
	for _, uidx := range dao.uidxAttrs {
		uname := strings.Join(uidx, "|")
		hashSlice1 := make([]string, 0)
		hashSlice2 := make([]string, 0)
		for _, f := range uidx {
			if v, err := bo.GboGetAttr(f, nil); err == nil {
				hashSlice1 = append(hashSlice1, fmt.Sprintf("%x", checksum.Checksum(dao.uidxHf1, v)))
				hashSlice2 = append(hashSlice2, fmt.Sprintf("%x", checksum.Checksum(dao.uidxHf2, v)))
			}
		}
		uhash := fmt.Sprintf("%x|%x", checksum.Checksum(dao.uidxHf1, hashSlice1), checksum.Checksum(dao.uidxHf2, hashSlice2))
		result[uname] = uhash
	}
	return result
}

// GdaoCreateFilter implements IGenericDao.GdaoCreateFilter.
//
// If dao.pkPrefix is specified, this function creates filter on compound PK as { dao.pkPrefix, FieldId }. Otherwise, filter on single-attribute PK as { FieldId } is created.
//
// If dao.pkPrefix is specified, this function first fetches value of attribute dao.pkPrefix from BO. If the fetched value is empty, dao.pkPrefixValue is used.
func (dao *UniversalDaoDynamodb) GdaoCreateFilter(_ string, bo godal.IGenericBo) godal.FilterOpt {
	filterMap := map[string]interface{}{FieldId: bo.GboGetAttrUnsafe(FieldId, reddo.TypeString)}
	if dao.pkPrefix != "" {
		v := bo.GboGetAttrUnsafe(dao.pkPrefix, reddo.TypeString)
		if v == nil || v == "" {
			v = dao.pkPrefixValue
		}
		filterMap[dao.pkPrefix] = v
	}
	return godal.MakeFilter(filterMap)
}

// ToUniversalBo transforms godal.IGenericBo to business object.
func (dao *UniversalDaoDynamodb) ToUniversalBo(gbo godal.IGenericBo) *UniversalBo {
	return NewUniversalBoFromGbo(gbo)
}

// ToGenericBo transforms business object to godal.IGenericBo.
func (dao *UniversalDaoDynamodb) ToGenericBo(ubo *UniversalBo) godal.IGenericBo {
	if ubo == nil {
		return nil
	}
	return ubo.ToGenericBo()
}

// Delete implements UniversalDao.Delete.
func (dao *UniversalDaoDynamodb) Delete(bo *UniversalBo) (bool, error) {
	gbo := dao.ToGenericBo(bo)
	if dao.uidxAttrs == nil || len(dao.uidxAttrs) == 0 {
		// go the easy way if there is no unique index
		numRows, err := dao.GdaoDelete(dao.tableName, gbo)
		return numRows > 0, err
	}

	pkAttrs := dao.GetRowMapper().ColumnsList(dao.tableName)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return false, fmt.Errorf("cannot find PK attribute list for table [%s]", dao.tableName)
	}
	keyFilter, err := toFilterMap(dao.GdaoCreateFilter(dao.tableName, gbo))
	if err != nil {
		return false, err
	}
	txItems := make([]*awsdynamodb.TransactWriteItem, 0)
	adc := dao.GetAwsDynamodbConnect()

	// step 1: delete record from the main table
	txItem, err := adc.BuildTxDelete(dao.tableName, keyFilter, nil)
	if err != nil {
		return false, err
	}
	condition := prom.AwsDynamodbExistsAllBuilder(pkAttrs)
	conditionExp, err := expression.NewBuilder().WithCondition(*condition).Build()
	if err != nil {
		return false, err
	}
	txItem.Delete.ConditionExpression = conditionExp.Condition()
	txItem.Delete.ExpressionAttributeNames = conditionExp.Names()
	txItem.Delete.ExpressionAttributeValues = conditionExp.Values()
	txItems = append(txItems, txItem)

	// step 2: delete record(s) from the uidx table
	uidxValues := dao.BuildUidxValues(gbo)
	for k, v := range uidxValues {
		keyFilterUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
		txItem, err := adc.BuildTxDelete(dao.uidxTableName, keyFilterUidx, nil)
		if err != nil {
			return false, err
		}
		txItems = append(txItems, txItem)
	}

	// wrap all steps inside a transaction
	_, err = adc.ExecTxWriteItems(nil, &awsdynamodb.TransactWriteItemsInput{TransactItems: txItems})
	if prom.IsAwsError(err, awsdynamodb.ErrCodeTransactionCanceledException) {
		return false, nil
	}
	return true, err
}

// Create implements UniversalDao.Create.
func (dao *UniversalDaoDynamodb) Create(bo *UniversalBo) (bool, error) {
	gbo := dao.ToGenericBo(bo)
	if dao.uidxAttrs == nil || len(dao.uidxAttrs) == 0 {
		// go the easy way if there is no unique index
		numRows, err := dao.GdaoCreate(dao.tableName, gbo)
		return numRows > 0, err
	}

	pkAttrs := dao.GetRowMapper().ColumnsList(dao.tableName)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return false, fmt.Errorf("cannot find PK attribute list for table [%s]", dao.tableName)
	}
	row, err := dao.GetRowMapper().ToRow(dao.tableName, gbo)
	if err != nil {
		return false, err
	}
	txItems := make([]*awsdynamodb.TransactWriteItem, 0)
	adc := dao.GetAwsDynamodbConnect()

	// step 1: insert record to the main table
	txItem, err := adc.BuildTxPutIfNotExist(dao.tableName, row, pkAttrs)
	if err != nil {
		return false, err
	}
	txItems = append(txItems, txItem)

	// step 2: insert record(s) to the uidx table
	uidxValues := dao.BuildUidxValues(gbo)
	pkAttrsUidx := []string{AwsDynamodbUidxTableColName, AwsDynamodbUidxTableColHash}
	for k, v := range uidxValues {
		rowUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
		for _, pkAttr := range pkAttrs {
			rowUidx[pkAttr] = gbo.GboGetAttrUnsafe(pkAttr, nil)
		}
		txItem, err := adc.BuildTxPutIfNotExist(dao.uidxTableName, rowUidx, pkAttrsUidx)
		if err != nil {
			return false, err
		}
		txItems = append(txItems, txItem)
	}

	// wrap all steps inside a transaction
	_, err = adc.ExecTxWriteItems(nil, &awsdynamodb.TransactWriteItemsInput{TransactItems: txItems})
	if awsErr, ok := err.(*awsdynamodb.TransactionCanceledException); ok {
		for _, reason := range awsErr.CancellationReasons {
			if *reason.Code == awsdynamodb.BatchStatementErrorCodeEnumConditionalCheckFailed {
				return false, godal.ErrGdaoDuplicatedEntry
			}
		}
	}
	return true, err
}

// Get implements UniversalDao.Get.
func (dao *UniversalDaoDynamodb) Get(id string) (*UniversalBo, error) {
	filterBo := NewUniversalBo(id, 0)
	gbo, err := dao.GdaoFetchOne(dao.tableName, dao.GdaoCreateFilter(dao.tableName, dao.ToGenericBo(filterBo)))
	if err != nil {
		return nil, err
	}
	return dao.ToUniversalBo(gbo), nil
}

// toConditionBuilder builds a ConditionBuilder from input.
//
//   - if input is expression.ConditionBuilder or *expression.ConditionBuilder: return it as *expression.ConditionBuilder.
// 	 - if input is string, slice/array of bytes: assume input is a map in JSON, convert it to map to build ConditionBuilder.
// 	 - if input is a map: build an "and" condition connecting sub-conditions where each sub-condition is an "equal" condition built from map entry.
func toConditionBuilder(input interface{}) (*expression.ConditionBuilder, error) {
	if input == nil {
		return nil, nil
	}
	switch input.(type) {
	case expression.ConditionBuilder:
		result := input.(expression.ConditionBuilder)
		return &result, nil
	case *expression.ConditionBuilder:
		return input.(*expression.ConditionBuilder), nil
	}
	v := reflect.ValueOf(input)
	for ; v.Kind() == reflect.Ptr; v = v.Elem() {
	}
	switch v.Kind() {
	case reflect.String:
		// expect input to be a map in JSON
		result := make(map[string]interface{})
		if err := json.Unmarshal([]byte(v.Interface().(string)), &result); err != nil {
			return nil, err
		}
		return toConditionBuilder(result)
	case reflect.Array, reflect.Slice:
		// expect input to be a map in JSON
		t, err := reddo.ToSlice(v.Interface(), reflect.TypeOf(byte(0)))
		if err != nil {
			return nil, err
		}
		result := make(map[string]interface{})
		if err := json.Unmarshal(t.([]byte), &result); err != nil {
			return nil, err
		}
		return toConditionBuilder(result)
	case reflect.Map:
		m, err := reddo.ToMap(v.Interface(), reflect.TypeOf(make(map[string]interface{})))
		if err != nil {
			return nil, err
		}
		var result *expression.ConditionBuilder = nil
		for k, v := range m.(map[string]interface{}) {
			if result == nil {
				t := expression.Name(k).Equal(expression.Value(v))
				result = &t
			} else {
				t := result.And(expression.Name(k).Equal(expression.Value(v)))
				result = &t
			}
		}
		return result, err
	}
	return nil, fmt.Errorf("cannot convert %v to *expression.ConditionBuilder", input)
}

// GetN implements UniversalDao.GetN.
func (dao *UniversalDaoDynamodb) GetN(fromOffset, maxNumRows int, filter godal.FilterOpt, sorting *godal.SortingOpt) ([]*UniversalBo, error) {
	// TODO AWS DynamoDB does not currently support custom sorting

	if dao.pkPrefix != "" && dao.pkPrefixValue != "" {
		/* multi-tenant: add tenant filtering */
		convertFilter, err := toConditionBuilder(filter)
		if err != nil {
			return nil, err
		}
		t := expression.Name(dao.pkPrefix).Equal(expression.Value(dao.pkPrefixValue))
		if convertFilter != nil {
			t = t.And(*convertFilter)
		}
		filter = &t
	}
	gboList, err := dao.GdaoFetchMany(dao.tableName, filter, sorting, fromOffset, maxNumRows)
	if err != nil {
		return nil, err
	}
	result := make([]*UniversalBo, 0)
	for _, gbo := range gboList {
		bo := dao.ToUniversalBo(gbo)
		result = append(result, bo)
	}
	return result, nil
}

// GetAll implements UniversalDao.GetAll.
func (dao *UniversalDaoDynamodb) GetAll(filter godal.FilterOpt, sorting *godal.SortingOpt) ([]*UniversalBo, error) {
	return dao.GetN(0, 0, filter, sorting)
}

// Update implements UniversalDao.Update.
func (dao *UniversalDaoDynamodb) Update(bo *UniversalBo) (bool, error) {
	gbo := dao.ToGenericBo(bo)
	if dao.uidxAttrs == nil || len(dao.uidxAttrs) == 0 {
		// go the easy way if there is no unique index
		numRows, err := dao.GdaoUpdate(dao.tableName, gbo)
		return numRows > 0, err
	}

	// cancel update if there is no existing row to update
	oldGbo, err := dao.GdaoFetchOne(dao.tableName, dao.GdaoCreateFilter(dao.tableName, dao.ToGenericBo(bo)))
	if err != nil {
		return false, err
	}
	if oldGbo == nil {
		return false, nil
	}

	pkAttrs := dao.GetRowMapper().ColumnsList(dao.tableName)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return false, fmt.Errorf("cannot find PK attribute list for table [%s]", dao.tableName)
	}
	keyFilter, err := toFilterMap(dao.GdaoCreateFilter(dao.tableName, gbo))
	if err != nil {
		return false, err
	}
	row, err := dao.GetRowMapper().ToRow(dao.tableName, gbo)
	if err != nil {
		return false, err
	}
	rowMap, ok := row.(map[string]interface{})
	if !ok || keyFilter == nil {
		return false, errors.New("row data must be a map")
	}
	// remove pk attributes from update list
	for _, pk := range pkAttrs {
		delete(rowMap, pk)
	}

	txItems := make([]*awsdynamodb.TransactWriteItem, 0)
	adc := dao.GetAwsDynamodbConnect()

	// step 1: update existing record in the main table
	condition := prom.AwsDynamodbExistsAllBuilder(pkAttrs)
	txItem, err := adc.BuildTxUpdate(dao.tableName, keyFilter, condition, nil, rowMap, nil, nil)
	if err != nil {
		return false, err
	}
	txItems = append(txItems, txItem)

	// step 2 & 3: remove existing records in the uidx table and insert updated ones
	oldUidxValues := dao.BuildUidxValues(oldGbo)
	uidxValues := dao.BuildUidxValues(gbo)
	pkAttrsUidx := []string{AwsDynamodbUidxTableColName, AwsDynamodbUidxTableColHash}
	for k, v := range oldUidxValues {
		if v != uidxValues[k] {
			keyFilterUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
			txItem, err := adc.BuildTxDelete(dao.uidxTableName, keyFilterUidx, nil)
			if err != nil {
				return false, err
			}
			txItems = append(txItems, txItem)
		}
	}
	for k, v := range uidxValues {
		if v != oldUidxValues[k] {
			rowUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
			for _, pkAttr := range pkAttrs {
				rowUidx[pkAttr] = gbo.GboGetAttrUnsafe(pkAttr, nil)
			}
			txItem, err := adc.BuildTxPutIfNotExist(dao.uidxTableName, rowUidx, pkAttrsUidx)
			if err != nil {
				return false, err
			}
			txItems = append(txItems, txItem)
		}
	}

	// wrap all steps inside a transaction
	_, err = adc.ExecTxWriteItems(nil, &awsdynamodb.TransactWriteItemsInput{TransactItems: txItems})
	if awsErr, ok := err.(*awsdynamodb.TransactionCanceledException); ok {
		for _, reason := range awsErr.CancellationReasons {
			if *reason.Code == awsdynamodb.BatchStatementErrorCodeEnumConditionalCheckFailed {
				return false, godal.ErrGdaoDuplicatedEntry
			}
		}
	}
	return true, err
}

// Save implements UniversalDao.Save.
func (dao *UniversalDaoDynamodb) Save(bo *UniversalBo) (bool, *UniversalBo, error) {
	existing, err := dao.Get(bo.GetId())
	if err != nil {
		return false, nil, err
	}

	gbo := dao.ToGenericBo(bo)
	if dao.uidxAttrs == nil || len(dao.uidxAttrs) == 0 {
		// go the easy way if there is no unique index
		numRows, err := dao.GdaoSave(dao.tableName, gbo)
		return numRows > 0, existing, err
	}

	oldGbo := dao.ToGenericBo(existing)
	pkAttrs := dao.GetRowMapper().ColumnsList(dao.tableName)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return false, existing, fmt.Errorf("cannot find PK attribute list for table [%s]", dao.tableName)
	}
	keyFilter, err := toFilterMap(dao.GdaoCreateFilter(dao.tableName, gbo))
	if err != nil {
		return false, existing, err
	}
	row, err := dao.GetRowMapper().ToRow(dao.tableName, gbo)
	if err != nil {
		return false, existing, err
	}
	rowMap, ok := row.(map[string]interface{})
	if !ok || keyFilter == nil {
		return false, existing, errors.New("row data must be a map")
	}

	txItems := make([]*awsdynamodb.TransactWriteItem, 0)
	adc := dao.GetAwsDynamodbConnect()

	// step 1: save existing record in the main table
	txItem, err := adc.BuildTxPut(dao.tableName, rowMap, nil)
	if err != nil {
		return false, nil, err
	}
	txItems = append(txItems, txItem)

	// step 2 & 3: remove existing records in the uidx table and insert updated ones
	oldUidxValues := dao.BuildUidxValues(oldGbo)
	uidxValues := dao.BuildUidxValues(gbo)
	pkAttrsUidx := []string{AwsDynamodbUidxTableColName, AwsDynamodbUidxTableColHash}
	for k, v := range oldUidxValues {
		if v != uidxValues[k] {
			keyFilterUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
			txItem, err := adc.BuildTxDelete(dao.uidxTableName, keyFilterUidx, nil)
			if err != nil {
				return false, existing, err
			}
			txItems = append(txItems, txItem)
		}
	}
	for k, v := range uidxValues {
		if v != oldUidxValues[k] {
			rowUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
			for _, pkAttr := range pkAttrs {
				rowUidx[pkAttr] = gbo.GboGetAttrUnsafe(pkAttr, nil)
			}
			txItem, err := adc.BuildTxPutIfNotExist(dao.uidxTableName, rowUidx, pkAttrsUidx)
			if err != nil {
				return false, existing, err
			}
			txItems = append(txItems, txItem)
		}
	}

	// wrap all steps inside a transaction
	_, err = adc.ExecTxWriteItems(nil, &awsdynamodb.TransactWriteItemsInput{TransactItems: txItems})
	if awsErr, ok := err.(*awsdynamodb.TransactionCanceledException); ok {
		for _, reason := range awsErr.CancellationReasons {
			if *reason.Code == awsdynamodb.BatchStatementErrorCodeEnumConditionalCheckFailed {
				return false, existing, godal.ErrGdaoDuplicatedEntry
			}
		}
	}
	return true, existing, err
}
