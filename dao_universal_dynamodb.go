package henge

import (
	"errors"
	"fmt"
	"strings"
	"time"

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
	AwsDynamodbUidxTableSuffix  = "_uidx"
	AwsDynamodbUidxTableColName = "uname"
	AwsDynamodbUidxTableColHash = "uhash"
)

// InitDynamodbTable initializes a DynamoDB table to store henge business objects.
//
// This function will create 2 tables. One with name 'tableName' to store business objects. The other one has
// the same base name and suffixed by AwsDynamodbUidxTableSuffix. The second table is used to manage unique indexes.
//
// The second table will be created with the same RCU/WCU and has the following schema {AwsDynamodbUidxTableColName:AwsDynamodbUidxTableColHash}
func InitDynamodbTable(adc *prom.AwsDynamodbConnect, tableName string, rcu, wcu int64) error {
	attrDefs := []prom.AwsDynamodbNameAndType{{FieldId, prom.AwsAttrTypeString}}
	pkDefs := []prom.AwsDynamodbNameAndType{{FieldId, prom.AwsKeyTypePartition}}
	err := adc.CreateTable(nil, tableName, rcu, wcu, attrDefs, pkDefs)
	if err = prom.AwsIgnoreErrorIfMatched(err, awsdynamodb.ErrCodeTableAlreadyExistsException); err != nil {
		return err
	}

	attrDefs = []prom.AwsDynamodbNameAndType{
		{AwsDynamodbUidxTableColName, prom.AwsAttrTypeString}, {AwsDynamodbUidxTableColHash, prom.AwsAttrTypeString},
	}
	pkDefs = []prom.AwsDynamodbNameAndType{
		{AwsDynamodbUidxTableColName, prom.AwsKeyTypePartition}, {AwsDynamodbUidxTableColHash, prom.AwsKeyTypeSort},
	}
	err = adc.CreateTable(nil, tableName+AwsDynamodbUidxTableSuffix, rcu, wcu, attrDefs, pkDefs)
	return prom.AwsIgnoreErrorIfMatched(err, awsdynamodb.ErrCodeTableAlreadyExistsException)
}

func buildRowMapperDynamodb(tableName string) godal.IRowMapper {
	return &rowMapperDynamodb{wrap: &dynamodb.GenericRowMapperDynamodb{
		ColumnsListMap: map[string][]string{tableName: {FieldId}},
	}}
}

// rowMapperDynamodb is an implementation of godal.IRowMapper specific for AWS DynamoDB.
type rowMapperDynamodb struct {
	wrap godal.IRowMapper
}

// ToRow implements godal.IRowMapper.ToRow
func (r *rowMapperDynamodb) ToRow(storageId string, bo godal.IGenericBo) (interface{}, error) {
	row, err := r.wrap.ToRow(storageId, bo)
	return row, err
}

// ToBo implements godal.IRowMapper.ToBo
func (r *rowMapperDynamodb) ToBo(storageId string, row interface{}) (godal.IGenericBo, error) {
	gbo, err := r.wrap.ToBo(storageId, row)
	return gbo, err
}

// ColumnsList implements godal.IRowMapper.ColumnsList
func (r *rowMapperDynamodb) ColumnsList(storageId string) []string {
	return r.wrap.ColumnsList(storageId)
}

// NewUniversalDaoDynamodb is helper method to create UniversalDaoDynamodb instance.
//
// - uidxAttrs list of unique indexes, each unique index is a combination of table columns.
func NewUniversalDaoDynamodb(adc *prom.AwsDynamodbConnect, tableName string, uidxAttrs [][]string) UniversalDao {
	dao := &UniversalDaoDynamodb{
		tableName:     tableName,
		uidxTableName: tableName + AwsDynamodbUidxTableSuffix,
		uidxAttrs:     uidxAttrs,
		uidxHf1:       checksum.Sha1HashFunc,
		uidxHf2:       checksum.Md5HashFunc,
	}
	dao.GenericDaoDynamodb = dynamodb.NewGenericDaoDynamodb(adc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(buildRowMapperDynamodb(tableName))
	return dao
}

// UniversalDaoDynamodb is AWS DynamoDB-based implementation of UniversalDao.
type UniversalDaoDynamodb struct {
	*dynamodb.GenericDaoDynamodb
	tableName        string            // name of database table to store business objects
	uidxTableName    string            // name of database table to store unique indexes
	uidxAttrs        [][]string        // list of unique indexes (each unique index is a combination of table columns)
	uidxHf1, uidxHf2 checksum.HashFunc // hash functions used to calculate unique index hash
}

// GetTableName returns name of database table to store business objects.
func (dao *UniversalDaoDynamodb) GetTableName() string {
	return dao.tableName
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
// Currently two hash functions are used, and two must be different. By default, the following hash fucntions
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
// The return value is a map {uidxName:uidxHashValue}
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
func (dao *UniversalDaoDynamodb) GdaoCreateFilter(_ string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{FieldId: bo.GboGetAttrUnsafe(FieldId, reddo.TypeString)}
}

// ToUniversalBo transforms godal.IGenericBo to business object.
func (dao *UniversalDaoDynamodb) ToUniversalBo(gbo godal.IGenericBo) *UniversalBo {
	if gbo == nil {
		return nil
	}
	extraFields := make(map[string]interface{})
	gbo.GboTransferViaJson(&extraFields)
	for _, field := range topLevelFieldList {
		delete(extraFields, field)
	}
	return &UniversalBo{
		id:          gbo.GboGetAttrUnsafe(FieldId, reddo.TypeString).(string),
		dataJson:    gbo.GboGetAttrUnsafe(FieldData, reddo.TypeString).(string),
		checksum:    gbo.GboGetAttrUnsafe(FieldChecksum, reddo.TypeString).(string),
		timeCreated: gbo.GboGetAttrUnsafe(FieldTimeCreated, reddo.TypeTime).(time.Time),
		timeUpdated: gbo.GboGetAttrUnsafe(FieldTimeUpdated, reddo.TypeTime).(time.Time),
		tagVersion:  gbo.GboGetAttrUnsafe(FieldTagVersion, reddo.TypeUint).(uint64),
		_extraAttrs: extraFields,
	}
}

// ToGenericBo transforms business object to godal.IGenericBo.
func (dao *UniversalDaoDynamodb) ToGenericBo(ubo *UniversalBo) godal.IGenericBo {
	if ubo == nil {
		return nil
	}
	gbo := godal.NewGenericBo()
	gbo.GboSetAttr(FieldId, ubo.id)
	gbo.GboSetAttr(FieldData, ubo.dataJson)
	gbo.GboSetAttr(FieldChecksum, ubo.checksum)
	gbo.GboSetAttr(FieldTimeCreated, ubo.timeCreated)
	gbo.GboSetAttr(FieldTimeUpdated, ubo.timeUpdated)
	gbo.GboSetAttr(FieldTagVersion, ubo.tagVersion)
	for k, v := range ubo._extraAttrs {
		gbo.GboSetAttr(k, v)
	}
	return gbo
}

// Delete implements UniversalDao.Delete.
func (dao *UniversalDaoDynamodb) Delete(bo *UniversalBo) (bool, error) {
	gbo := dao.ToGenericBo(bo.Clone())
	if dao.uidxAttrs == nil || len(dao.uidxAttrs) == 0 {
		// go the easy way if there is no unique index
		numRows, err := dao.GdaoDelete(dao.tableName, gbo)
		return numRows > 0, err
	}

	pkAttrs := dao.GetRowMapper().ColumnsList(dao.tableName)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return false, errors.New(fmt.Sprintf("cannot find primary-key attribute list for table [%s]", dao.tableName))
	}
	keyFilter, ok := dao.GdaoCreateFilter(dao.tableName, gbo).(map[string]interface{})
	if !ok || keyFilter == nil {
		return false, errors.New("cannot build filter to delete row")
	}
	txItems := make([]*awsdynamodb.TransactWriteItem, 0)
	adc := dao.GetAwsDynamodbConnect()

	// step 1: delete record from the main table
	if txItem, err := adc.BuildTxDelete(dao.tableName, keyFilter, nil); err != nil {
		return false, err
	} else {
		condition := prom.AwsDynamodbExistsAllBuilder(pkAttrs)
		if conditionExp, err := expression.NewBuilder().WithCondition(*condition).Build(); err != nil {
			return false, err
		} else {
			txItem.Delete.ConditionExpression = conditionExp.Condition()
			txItem.Delete.ExpressionAttributeNames = conditionExp.Names()
			txItem.Delete.ExpressionAttributeValues = conditionExp.Values()
		}
		txItems = append(txItems, txItem)
	}

	// step 2: delete record(s) from the uidx table
	uidxValues := dao.BuildUidxValues(gbo)
	for k, v := range uidxValues {
		keyFilterUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
		if txItem, err := adc.BuildTxDelete(dao.uidxTableName, keyFilterUidx, nil); err != nil {
			return false, err
		} else {
			txItems = append(txItems, txItem)
		}
	}

	// wrap all steps inside a transaction
	_, err := adc.ExecTxWriteItems(nil, &awsdynamodb.TransactWriteItemsInput{TransactItems: txItems})
	if prom.IsAwsError(err, awsdynamodb.ErrCodeTransactionCanceledException) {
		return false, nil
	}
	return true, err
}

// Create implements UniversalDao.Create.
func (dao *UniversalDaoDynamodb) Create(bo *UniversalBo) (bool, error) {
	gbo := dao.ToGenericBo(bo.Clone())
	if dao.uidxAttrs == nil || len(dao.uidxAttrs) == 0 {
		// go the easy way if there is no unique index
		numRows, err := dao.GdaoCreate(dao.tableName, gbo)
		return numRows > 0, err
	}

	pkAttrs := dao.GetRowMapper().ColumnsList(dao.tableName)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return false, errors.New(fmt.Sprintf("cannot find primary-key attribute list for table [%s]", dao.tableName))
	}
	row, err := dao.GetRowMapper().ToRow(dao.tableName, gbo)
	if err != nil {
		return false, err
	}
	txItems := make([]*awsdynamodb.TransactWriteItem, 0)
	adc := dao.GetAwsDynamodbConnect()

	// step 1: insert record to the main table
	if txItem, err := adc.BuildTxPutIfNotExist(dao.tableName, row, pkAttrs); err != nil {
		return false, err
	} else {
		txItems = append(txItems, txItem)
	}

	// step 2: insert record(s) to the uidx table
	uidxValues := dao.BuildUidxValues(gbo)
	pkAttrsUidx := []string{AwsDynamodbUidxTableColName, AwsDynamodbUidxTableColHash}
	for k, v := range uidxValues {
		rowUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
		for _, pkAttr := range pkAttrs {
			rowUidx[pkAttr] = gbo.GboGetAttrUnsafe(pkAttr, nil)
		}
		if txItem, err := adc.BuildTxPutIfNotExist(dao.uidxTableName, rowUidx, pkAttrsUidx); err != nil {
			return false, err
		} else {
			txItems = append(txItems, txItem)
		}
	}

	// wrap all steps inside a transaction
	_, err = adc.ExecTxWriteItems(nil, &awsdynamodb.TransactWriteItemsInput{TransactItems: txItems})
	if awsErr, ok := err.(*awsdynamodb.TransactionCanceledException); ok {
		for _, reason := range awsErr.CancellationReasons {
			if *reason.Code == "ConditionalCheckFailed" {
				return false, godal.GdaoErrorDuplicatedEntry
			}
		}
	}
	return true, err
}

// Get implements UniversalDao.Get.
func (dao *UniversalDaoDynamodb) Get(id string) (*UniversalBo, error) {
	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{FieldId: id})
	if err != nil {
		return nil, err
	}
	return dao.ToUniversalBo(gbo), nil
}

// GetN implements UniversalDao.GetN.
func (dao *UniversalDaoDynamodb) GetN(fromOffset, maxNumRows int, filter interface{}, sorting interface{}) ([]*UniversalBo, error) {
	// TODO AWS DynamoDB does not currently support custom sorting

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
func (dao *UniversalDaoDynamodb) GetAll(filter interface{}, sorting interface{}) ([]*UniversalBo, error) {
	return dao.GetN(0, 0, filter, sorting)
}

// Update implements UniversalDao.Update.
func (dao *UniversalDaoDynamodb) Update(bo *UniversalBo) (bool, error) {
	gbo := dao.ToGenericBo(bo.Clone())
	if dao.uidxAttrs == nil || len(dao.uidxAttrs) == 0 {
		// go the easy way if there is no unique index
		numRows, err := dao.GdaoUpdate(dao.tableName, gbo)
		return numRows > 0, err
	}

	// cancel update if there is no existing row to update
	oldGbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{FieldId: bo.id})
	if err != nil {
		return false, err
	}
	if oldGbo == nil {
		return false, nil
	}

	pkAttrs := dao.GetRowMapper().ColumnsList(dao.tableName)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return false, errors.New(fmt.Sprintf("cannot find primary-key attribute list for table [%s]", dao.tableName))
	}
	keyFilter, ok := dao.GdaoCreateFilter(dao.tableName, gbo).(map[string]interface{})
	if !ok || keyFilter == nil {
		return false, errors.New("cannot build filter to update row")
	}
	row, err := dao.GetRowMapper().ToRow(dao.tableName, gbo)
	if err != nil {
		return false, err
	}
	rowMap, ok := row.(map[string]interface{})
	if !ok || keyFilter == nil {
		return false, errors.New("row data must be a map")
	}
	// remove primary key attributes from update list
	for _, pk := range pkAttrs {
		delete(rowMap, pk)
	}

	txItems := make([]*awsdynamodb.TransactWriteItem, 0)
	adc := dao.GetAwsDynamodbConnect()

	// step 1: update existing record in the main table
	condition := prom.AwsDynamodbExistsAllBuilder(pkAttrs)
	if txItem, err := adc.BuildTxUpdate(dao.tableName, keyFilter, condition, nil, rowMap, nil, nil); err != nil {
		return false, err
	} else {
		txItems = append(txItems, txItem)
	}

	// step 2 & 3: remove existing records in the uidx table and insert updated ones
	oldUidxValues := dao.BuildUidxValues(oldGbo)
	uidxValues := dao.BuildUidxValues(gbo)
	pkAttrsUidx := []string{AwsDynamodbUidxTableColName, AwsDynamodbUidxTableColHash}
	for k, v := range oldUidxValues {
		if v != uidxValues[k] {
			keyFilterUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
			if txItem, err := adc.BuildTxDelete(dao.uidxTableName, keyFilterUidx, nil); err != nil {
				return false, err
			} else {
				txItems = append(txItems, txItem)
			}
		}
	}
	for k, v := range uidxValues {
		if v != oldUidxValues[k] {
			rowUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
			for _, pkAttr := range pkAttrs {
				rowUidx[pkAttr] = gbo.GboGetAttrUnsafe(pkAttr, nil)
			}
			if txItem, err := adc.BuildTxPutIfNotExist(dao.uidxTableName, rowUidx, pkAttrsUidx); err != nil {
				return false, err
			} else {
				txItems = append(txItems, txItem)
			}
		}
	}

	// wrap all steps inside a transaction
	_, err = adc.ExecTxWriteItems(nil, &awsdynamodb.TransactWriteItemsInput{TransactItems: txItems})
	if awsErr, ok := err.(*awsdynamodb.TransactionCanceledException); ok {
		for _, reason := range awsErr.CancellationReasons {
			if *reason.Code == "ConditionalCheckFailed" {
				return false, godal.GdaoErrorDuplicatedEntry
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

	gbo := dao.ToGenericBo(bo.Clone())
	if dao.uidxAttrs == nil || len(dao.uidxAttrs) == 0 {
		// go the easy way if there is no unique index
		numRows, err := dao.GdaoSave(dao.tableName, gbo)
		return numRows > 0, existing, err
	}

	oldGbo := dao.ToGenericBo(existing)
	pkAttrs := dao.GetRowMapper().ColumnsList(dao.tableName)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return false, existing, errors.New(fmt.Sprintf("cannot find primary-key attribute list for table [%s]", dao.tableName))
	}
	keyFilter, ok := dao.GdaoCreateFilter(dao.tableName, gbo).(map[string]interface{})
	if !ok || keyFilter == nil {
		return false, existing, errors.New("cannot build filter to save row")
	}
	row, err := dao.GetRowMapper().ToRow(dao.tableName, gbo)
	if err != nil {
		return false, existing, err
	}
	rowMap, ok := row.(map[string]interface{})
	if !ok || keyFilter == nil {
		return false, existing, errors.New("row data must be a map")
	}
	// // remove primary key attributes from update list
	// for _, pk := range pkAttrs {
	// 	delete(rowMap, pk)
	// }

	txItems := make([]*awsdynamodb.TransactWriteItem, 0)
	adc := dao.GetAwsDynamodbConnect()

	// step 1: save existing record in the main table
	if txItem, err := adc.BuildTxPut(dao.tableName, rowMap, nil); err != nil {
		return false, nil, err
	} else {
		txItems = append(txItems, txItem)
	}

	// step 2 & 3: remove existing records in the uidx table and insert updated ones
	oldUidxValues := dao.BuildUidxValues(oldGbo)
	uidxValues := dao.BuildUidxValues(gbo)
	pkAttrsUidx := []string{AwsDynamodbUidxTableColName, AwsDynamodbUidxTableColHash}
	for k, v := range oldUidxValues {
		if v != uidxValues[k] {
			keyFilterUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
			if txItem, err := adc.BuildTxDelete(dao.uidxTableName, keyFilterUidx, nil); err != nil {
				return false, existing, err
			} else {
				txItems = append(txItems, txItem)
			}
		}
	}
	for k, v := range uidxValues {
		if v != oldUidxValues[k] {
			rowUidx := map[string]interface{}{AwsDynamodbUidxTableColName: k, AwsDynamodbUidxTableColHash: v}
			for _, pkAttr := range pkAttrs {
				rowUidx[pkAttr] = gbo.GboGetAttrUnsafe(pkAttr, nil)
			}
			if txItem, err := adc.BuildTxPutIfNotExist(dao.uidxTableName, rowUidx, pkAttrsUidx); err != nil {
				return false, existing, err
			} else {
				txItems = append(txItems, txItem)
			}
		}
	}

	// wrap all steps inside a transaction
	_, err = adc.ExecTxWriteItems(nil, &awsdynamodb.TransactWriteItemsInput{TransactItems: txItems})
	if awsErr, ok := err.(*awsdynamodb.TransactionCanceledException); ok {
		for _, reason := range awsErr.CancellationReasons {
			if *reason.Code == "ConditionalCheckFailed" {
				return false, existing, godal.GdaoErrorDuplicatedEntry
			}
		}
	}
	return true, existing, err
}
