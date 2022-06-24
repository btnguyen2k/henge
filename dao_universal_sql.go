package henge

import (
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/sql"
	"github.com/btnguyen2k/prom"
)

// NewSqlConnection is convenient function to create prom.SqlConnect instance.
//
// Note: it's application's responsibility to import proper SQL driver and supply the correct driver.
//
// Note: timezone is default to UTC if not supplied.
//
// Available: since v0.3.0
func NewSqlConnection(url, timezone, driver string, dbFlavor prom.DbFlavor, defaultTimeoutMs int, poolOptions *prom.SqlPoolOptions) (*prom.SqlConnect, error) {
	sqlc, err := prom.NewSqlConnect(driver, url, defaultTimeoutMs, poolOptions)
	if err != nil {
		return nil, err
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	sqlc.SetLocation(loc).SetDbFlavor(dbFlavor)
	return sqlc, nil
}

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
//   - txModeOnWrite: enables/disables transaction mode on write operations.
//       RDBMS/SQL's implementation of GdaoSave is "try update, if failed then insert".
//       It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.
//       Recommended setting is "txModeOnWrite=true".
//   - defaultUboOpts: (since v0.5.7) the default options to be used by the DAO when creating UniversalBo instances.
func NewUniversalDaoSql(sqlc *prom.SqlConnect, tableName string, txModeOnWrite bool,
	extraColNameToFieldMappings map[string]string, defaultUboOpts ...UboOpt) UniversalDao {
	dao := &UniversalDaoSql{
		tableName:              tableName,
		funcFilterGeneratorSql: defaultFilterGeneratorSql,
		defaultSorting:         (&godal.SortingField{FieldName: FieldId}).ToSortingOpt(),
		defaultUboOpts:         defaultUboOpts,
	}
	dao.IGenericDaoSql = sql.NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(buildRowMapperSql(tableName, extraColNameToFieldMappings))
	dao.SetTxModeOnWrite(txModeOnWrite).SetSqlFlavor(sqlc.GetDbFlavor())
	dao.Init()
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

// FuncFilterGeneratorSql defines an API to generate filter for universal BO, to be used with UniversalDaoSql.
//
// input can be either UniversalBo, *UniversalBo, godal.IGenericBo or godal.FilterOpt instance.
//
// Available: since v0.3.0
type FuncFilterGeneratorSql func(tableName string, input interface{}) godal.FilterOpt

// defaultFilterGeneratorSql is the default instance of FuncFilterGeneratorSql.
func defaultFilterGeneratorSql(_ string, input interface{}) godal.FilterOpt {
	switch input.(type) {
	case UniversalBo:
		bo := input.(UniversalBo)
		return godal.MakeFilter(map[string]interface{}{FieldId: bo.id})
	case *UniversalBo:
		bo := input.(*UniversalBo)
		return godal.MakeFilter(map[string]interface{}{FieldId: bo.id})
	}
	if gbo, ok := input.(godal.IGenericBo); ok && gbo != nil {
		return godal.MakeFilter(map[string]interface{}{FieldId: gbo.GboGetAttrUnsafe(FieldId, reddo.TypeString)})
	}
	if filter, ok := input.(godal.FilterOpt); ok {
		return filter
	}
	return input
}

// UniversalDaoSql is SQL-based implementation of UniversalDao.
type UniversalDaoSql struct {
	sql.IGenericDaoSql
	tableName              string
	funcFilterGeneratorSql FuncFilterGeneratorSql
	defaultSorting         *godal.SortingOpt
	defaultUboOpts         []UboOpt // (since v0.5.7) default options used by the DAO to create UniversalBo instances
}

// Init should be called to initialize the DAO instance before use.
//
// Available since v0.5.7
func (dao *UniversalDaoSql) Init() error {
	if len(dao.defaultUboOpts) == 0 {
		uboOpt := UboOpt{TimeLayout: time.RFC3339, TimestampRounding: TimestampRoundingSettingSecond}
		switch dao.GetSqlFlavor() {
		case prom.FlavorCosmosDb:
			uboOpt = UboOpt{TimeLayout: time.RFC3339Nano, TimestampRounding: TimestampRoundingSettingNanosecond}
		case prom.FlavorSqlite:
			uboOpt = UboOpt{TimeLayout: "2006-01-02 15:04:05Z07:00", TimestampRounding: TimestampRoundingSettingSecond}
		}
		dao.SetDefaultUboOpts([]UboOpt{uboOpt})
	}
	if dao.GetFuncFilterGeneratorSql() == nil {
		dao.SetFuncFilterGeneratorSql(defaultFilterGeneratorSql)
	}
	if dao.GetDefaultSorting() == nil {
		dao.SetDefaultSorting((&godal.SortingField{FieldName: FieldId}).ToSortingOpt())
	}
	return nil
}

// GetDefaultUboOpts returns the default options to be used by the DAO when creating UniversalBo instances.
//
// Available since v0.5.7
func (dao *UniversalDaoSql) GetDefaultUboOpts() []UboOpt {
	return dao.defaultUboOpts
}

// SetDefaultUboOpts sets the default options to be used by the DAO when creating UniversalBo instances.
//
// Available since v0.5.7
func (dao *UniversalDaoSql) SetDefaultUboOpts(uboOpts []UboOpt) *UniversalDaoSql {
	dao.defaultUboOpts = uboOpts
	return dao
}

// GetFuncFilterGeneratorSql returns the function used to generate filter for universal BO.
//
// See FuncFilterGeneratorSql for more information.
//
// Available since v0.5.7
func (dao *UniversalDaoSql) GetFuncFilterGeneratorSql() FuncFilterGeneratorSql {
	return dao.funcFilterGeneratorSql
}

// SetFuncFilterGeneratorSql returns the function used to generate filter for universal BO.
//
// See FuncFilterGeneratorSql for more information.
//
// Available since v0.5.7
func (dao *UniversalDaoSql) SetFuncFilterGeneratorSql(funcFilterGeneratorSql FuncFilterGeneratorSql) *UniversalDaoSql {
	dao.funcFilterGeneratorSql = funcFilterGeneratorSql
	return dao
}

// GetDefaultSorting returns the default sorting option to be used for querying BOs.
//
// See FuncFilterGeneratorSql for more information.
//
// Available since v0.5.7
func (dao *UniversalDaoSql) GetDefaultSorting() *godal.SortingOpt {
	return dao.defaultSorting
}

// SetDefaultSorting sets the default sorting option to be used for querying BOs.
//
// See FuncFilterGeneratorSql for more information.
//
// Available since v0.5.7
func (dao *UniversalDaoSql) SetDefaultSorting(defaultSorting *godal.SortingOpt) *UniversalDaoSql {
	dao.defaultSorting = defaultSorting
	return dao
}

// GdaoCreateFilter implements IGenericDao.GdaoCreateFilter.
func (dao *UniversalDaoSql) GdaoCreateFilter(tableName string, bo godal.IGenericBo) godal.FilterOpt {
	if dao.funcFilterGeneratorSql == nil {
		dao.funcFilterGeneratorSql = defaultFilterGeneratorSql
	}
	return dao.funcFilterGeneratorSql(tableName, bo)
}

// ToUniversalBo transforms godal.IGenericBo to business object.
func (dao *UniversalDaoSql) ToUniversalBo(gbo godal.IGenericBo) *UniversalBo {
	return NewUniversalBoFromGbo(gbo, dao.defaultUboOpts...)
}

// ToGenericBo transforms business object to godal.IGenericBo.
func (dao *UniversalDaoSql) ToGenericBo(ubo *UniversalBo) godal.IGenericBo {
	if ubo == nil {
		return nil
	}
	return ubo.ToGenericBo()
}

// Delete implements UniversalDao.Delete.
func (dao *UniversalDaoSql) Delete(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoDelete(dao.tableName, dao.ToGenericBo(bo))
	return numRows > 0, err
}

// Create implements UniversalDao.Create.
func (dao *UniversalDaoSql) Create(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoCreate(dao.tableName, dao.ToGenericBo(bo))
	return numRows > 0, err
}

// Get implements UniversalDao.Get.
func (dao *UniversalDaoSql) Get(id string) (*UniversalBo, error) {
	filterBo := &UniversalBo{id: id, _dirty: false}
	filterGbo := dao.ToGenericBo(filterBo)
	gbo, err := dao.GdaoFetchOne(dao.tableName, dao.GdaoCreateFilter(dao.tableName, filterGbo))
	if err != nil {
		return nil, err
	}
	return dao.ToUniversalBo(gbo), nil
}

// GetN implements UniversalDao.GetN.
func (dao *UniversalDaoSql) GetN(fromOffset, maxNumRows int, filter godal.FilterOpt, sorting *godal.SortingOpt) ([]*UniversalBo, error) {
	if sorting == nil {
		sorting = dao.defaultSorting
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
func (dao *UniversalDaoSql) GetAll(filter godal.FilterOpt, sorting *godal.SortingOpt) ([]*UniversalBo, error) {
	return dao.GetN(0, 0, filter, sorting)
}

// Update implements UniversalDao.Update.
func (dao *UniversalDaoSql) Update(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoUpdate(dao.tableName, dao.ToGenericBo(bo))
	return numRows > 0, err
}

// Save implements UniversalDao.Save.
func (dao *UniversalDaoSql) Save(bo *UniversalBo) (bool, *UniversalBo, error) {
	existing, err := dao.Get(bo.GetId())
	if err != nil {
		return false, nil, err
	}
	numRows, err := dao.GdaoSave(dao.tableName, dao.ToGenericBo(bo))
	return numRows > 0, existing, err
}
