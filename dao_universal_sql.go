package henge

import (
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/sql"
	"github.com/btnguyen2k/prom"
)

func buildRowMapperSql(tableName string, extraColNameToFieldMappings map[string]string) godal.IRowMapper {
	myCols := append([]string{}, sqlColumnNames...)
	myMapFieldToColName := cloneMap(sqlMapFieldToColName)
	myMapColNameToField := cloneMap(sqlMapColNameToField)
	for col, field := range extraColNameToFieldMappings {
		myCols = append(myCols, col)
		myMapColNameToField[col] = field
		myMapFieldToColName[field] = col
	}
	return &sql.GenericRowMapperSql{
		NameTransformation:          sql.NameTransfLowerCase,
		GboFieldToColNameTranslator: map[string]map[string]interface{}{tableName: myMapFieldToColName},
		ColNameToGboFieldTranslator: map[string]map[string]interface{}{tableName: myMapColNameToField},
		ColumnsListMap:              map[string][]string{tableName: myCols},
	}
}

// NewUniversalDaoSql is helper method to create UniversalDaoSql instance.
//
// - txModeOnWrite: enables/disables transaction mode on write operations.
//       RDBMS/SQL's implementation of GdaoSave is "try update, if failed then insert".
//       It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.
//       Recommended setting is "txModeOnWrite=true".
func NewUniversalDaoSql(sqlc *prom.SqlConnect, tableName string, txModeOnWrite bool, extraColNameToFieldMappings map[string]string) UniversalDao {
	dao := &UniversalDaoSql{tableName: tableName}
	dao.GenericDaoSql = sql.NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(buildRowMapperSql(tableName, extraColNameToFieldMappings))
	dao.SetSqlFlavor(sqlc.GetDbFlavor())
	dao.SetTxModeOnWrite(txModeOnWrite)
	return dao
}

const (
	// SqlColId is name of table column to store BO's id.
	SqlColId = "zid"
	// SqlColData is name of table column to store BO's user-defined attributes in JSON format.
	SqlColData = "zdata"
	// SqlColChecksum is name of table column to store checksum of BO's value.
	SqlColChecksum = "zchecksum"
	// SqlColTimeCreated is name of table column to store BO's creation timestamp.
	SqlColTimeCreated = "ztcreated"
	// SqlColTimeUpdated is name of table column to store BO's last-updated timestamp.
	SqlColTimeUpdated = "ztupdated"
	// SqlColTagVersion is name of table column to store BO's "tag-version" - a value that can be used for compatibility check or data migration.
	SqlColTagVersion = "ztversion"
)

var (
	sqlColumnNames       = []string{SqlColId, SqlColData, SqlColTagVersion, SqlColChecksum, SqlColTimeCreated, SqlColTimeUpdated}
	sqlMapFieldToColName = map[string]interface{}{
		FieldId:          SqlColId,
		FieldData:        SqlColData,
		FieldTagVersion:  SqlColTagVersion,
		FieldChecksum:    SqlColChecksum,
		FieldTimeCreated: SqlColTimeCreated,
		FieldTimeUpdated: SqlColTimeUpdated,
	}
	sqlMapColNameToField = map[string]interface{}{
		SqlColId:          FieldId,
		SqlColData:        FieldData,
		SqlColTagVersion:  FieldTagVersion,
		SqlColChecksum:    FieldChecksum,
		SqlColTimeCreated: FieldTimeCreated,
		SqlColTimeUpdated: FieldTimeUpdated,
	}
)

// UniversalDaoSql is SQL-based implementation of UniversalDao.
type UniversalDaoSql struct {
	*sql.GenericDaoSql
	tableName string
}

// GdaoCreateFilter implements IGenericDao.GdaoCreateFilter.
func (dao *UniversalDaoSql) GdaoCreateFilter(_ string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{SqlColId: bo.GboGetAttrUnsafe(FieldId, reddo.TypeString)}
}

// ToUniversalBo transforms godal.IGenericBo to business object.
func (dao *UniversalDaoSql) ToUniversalBo(gbo godal.IGenericBo) *UniversalBo {
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
func (dao *UniversalDaoSql) ToGenericBo(ubo *UniversalBo) godal.IGenericBo {
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
func (dao *UniversalDaoSql) Delete(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoDelete(dao.tableName, dao.ToGenericBo(bo))
	return numRows > 0, err
}

// Create implements UniversalDao.Create.
func (dao *UniversalDaoSql) Create(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoCreate(dao.tableName, dao.ToGenericBo(bo.Clone()))
	return numRows > 0, err
}

// Get implements UniversalDao.Get.
func (dao *UniversalDaoSql) Get(id string) (*UniversalBo, error) {
	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{SqlColId: id})
	if err != nil {
		return nil, err
	}
	return dao.ToUniversalBo(gbo), nil
}

// GetN implements UniversalDao.GetN.
func (dao *UniversalDaoSql) GetN(fromOffset, maxNumRows int, filter interface{}, sorting interface{}) ([]*UniversalBo, error) {
	if sorting == nil {
		// default sorting: ascending by "id" column
		sorting = (&sql.GenericSorting{Flavor: dao.GetSqlFlavor()}).Add(SqlColId)
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
func (dao *UniversalDaoSql) GetAll(filter interface{}, sorting interface{}) ([]*UniversalBo, error) {
	return dao.GetN(0, 0, filter, sorting)
}

// Update implements UniversalDao.Update.
func (dao *UniversalDaoSql) Update(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoUpdate(dao.tableName, dao.ToGenericBo(bo.Clone()))
	return numRows > 0, err
}

// Save implements UniversalDao.Save.
func (dao *UniversalDaoSql) Save(bo *UniversalBo) (bool, *UniversalBo, error) {
	existing, err := dao.Get(bo.GetId())
	if err != nil {
		return false, nil, err
	}
	numRows, err := dao.GdaoSave(dao.tableName, dao.ToGenericBo(bo.Clone()))
	return numRows > 0, existing, err
}
