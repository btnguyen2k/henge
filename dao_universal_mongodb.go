package henge

import (
	"encoding/json"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/mongo"
	prom "github.com/btnguyen2k/prom/mongo"
)

// InitMongoCollection initializes a MongoDB collection to store henge business objects.
//   - This function creates the specified collection with default settings.
//   - Other than the collection, no index is created.
func InitMongoCollection(mc *prom.MongoConnect, collectionName string) error {
	return mc.CreateCollection(collectionName)
}

func buildRowMapperMongo() godal.IRowMapper {
	return &rowMapperMongo{mongo.GenericRowMapperMongoInstance}
}

// rowMapperMongo is an implementation of godal.IRowMapper specific for MongoDB.
type rowMapperMongo struct {
	godal.IRowMapper
}

// ToRow implements godal.IRowMapper.ToRow.
func (r *rowMapperMongo) ToRow(storageId string, bo godal.IGenericBo) (interface{}, error) {
	row, err := r.IRowMapper.ToRow(storageId, bo)
	if m, ok := row.(map[string]interface{}); err == nil && ok && m != nil {
		if MongoColId != FieldId {
			m[MongoColId] = m[FieldId]
			delete(m, FieldId)
		}
		m[FieldTagVersion], _ = bo.GboGetAttr(FieldTagVersion, nil) // tag-version should be integer
		m[FieldTimeCreated], _ = bo.GboGetTimeWithLayout(FieldTimeCreated, time.RFC3339)
		m[FieldTimeUpdated], _ = bo.GboGetTimeWithLayout(FieldTimeUpdated, time.RFC3339)
		m[FieldData], _ = bo.GboGetAttrUnmarshalJson(FieldData) // Note: FieldData must be JSON-encoded string!
	}
	return row, err
}

// ToBo implements godal.IRowMapper.ToBo.
func (r *rowMapperMongo) ToBo(storageId string, row interface{}) (godal.IGenericBo, error) {
	gbo, err := r.IRowMapper.ToBo(storageId, row)
	if err == nil && gbo != nil {
		if MongoColId != FieldId {
			v, _ := gbo.GboGetAttr(MongoColId, nil)
			gbo.GboSetAttr(MongoColId, nil)
			gbo.GboSetAttr(FieldId, v)
		}
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

// NewUniversalDaoMongo is helper method to create UniversalDaoMongo instance.
//   - txModeOnWrite: enables/disables transaction mode on write operations.
//       MongoDB's implementation of GdaoCreate is "get/check and write".
//       It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.
//       As of MongoDB 4.0, transactions are available for replica set deployments only. Since MongoDB 4.2, transactions are also available for sharded cluster.
//       It is recommended to set "txModeOnWrite=true" whenever possible.
//   - defaultUboOpts: (since v0.5.7) the default options to be used by the DAO when creating UniversalBo instances.
func NewUniversalDaoMongo(mc *prom.MongoConnect, collectionName string, txModeOnWrite bool, defaultUboOpts ...UboOpt) UniversalDao {
	dao := &UniversalDaoMongo{
		collectionName: collectionName,
		defaultUboOpts: defaultUboOpts,
	}
	dao.GenericDaoMongo = mongo.NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(buildRowMapperMongo())
	dao.SetTxModeOnWrite(txModeOnWrite)
	dao.Init()
	return dao
}

const (
	// MongoColId holds the name of MongoDB collection's "id" field.
	MongoColId = "_id"
)

// UniversalDaoMongo is MongoDB-based implementation of UniversalDao.
type UniversalDaoMongo struct {
	*mongo.GenericDaoMongo
	collectionName string   // name of the MongoDB collection to store business objects
	defaultUboOpts []UboOpt // (since v0.5.7) default options used by the DAO to create UniversalBo instances
}

// Init should be called to initialize the DAO instance before use.
//
// Available since v0.5.7
func (dao *UniversalDaoMongo) Init() error {
	if len(dao.defaultUboOpts) == 0 {
		uboOpt := UboOpt{TimeLayout: time.RFC3339, TimestampRounding: TimestampRoundingSettingSecond}
		dao.SetDefaultUboOpts([]UboOpt{uboOpt})
	}
	if dao.GetRowMapper() == nil {
		dao.SetRowMapper(buildRowMapperMongo())
	}
	return nil
}

// GetDefaultUboOpts returns the default options to be used by the DAO when creating UniversalBo instances.
//
// Available since v0.5.7
func (dao *UniversalDaoMongo) GetDefaultUboOpts() []UboOpt {
	return dao.defaultUboOpts
}

// SetDefaultUboOpts sets the default options to be used by the DAO when creating UniversalBo instances.
//
// Available since v0.5.7
func (dao *UniversalDaoMongo) SetDefaultUboOpts(uboOpts []UboOpt) *UniversalDaoMongo {
	dao.defaultUboOpts = uboOpts
	return dao
}

// GdaoCreateFilter implements IGenericDao.GdaoCreateFilter.
func (dao *UniversalDaoMongo) GdaoCreateFilter(_ string, bo godal.IGenericBo) godal.FilterOpt {
	return godal.MakeFilter(map[string]interface{}{MongoColId: bo.GboGetAttrUnsafe(FieldId, reddo.TypeString)})
}

// ToUniversalBo implements UniversalDao.ToUniversalBo.
func (dao *UniversalDaoMongo) ToUniversalBo(gbo godal.IGenericBo) *UniversalBo {
	return NewUniversalBoFromGbo(gbo, dao.defaultUboOpts...)
}

// ToGenericBo implements UniversalDao.ToGenericBo.
func (dao *UniversalDaoMongo) ToGenericBo(ubo *UniversalBo) godal.IGenericBo {
	if ubo == nil {
		return nil
	}
	return ubo.ToGenericBo()
}

// Delete implements UniversalDao.Delete.
func (dao *UniversalDaoMongo) Delete(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoDelete(dao.collectionName, dao.ToGenericBo(bo))
	return numRows > 0, err
}

// Create implements UniversalDao.Create.
func (dao *UniversalDaoMongo) Create(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoCreate(dao.collectionName, dao.ToGenericBo(bo))
	return numRows > 0, err
}

// Get implements UniversalDao.Get.
func (dao *UniversalDaoMongo) Get(id string) (*UniversalBo, error) {
	filterBo := NewUniversalBo(id, 0)
	filter := dao.GdaoCreateFilter(dao.collectionName, filterBo.ToGenericBo())
	gbo, err := dao.GdaoFetchOne(dao.collectionName, filter)
	if err != nil {
		return nil, err
	}
	return dao.ToUniversalBo(gbo), nil
}

// GetN implements UniversalDao.GetN.
func (dao *UniversalDaoMongo) GetN(fromOffset, maxNumRows int, filter godal.FilterOpt, sorting *godal.SortingOpt) ([]*UniversalBo, error) {
	if sorting == nil {
		// default sorting: ascending by "id" column
		sorting = (&godal.SortingField{FieldName: MongoColId}).ToSortingOpt()
	}
	gboList, err := dao.GdaoFetchMany(dao.collectionName, filter, sorting, fromOffset, maxNumRows)
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
func (dao *UniversalDaoMongo) GetAll(filter godal.FilterOpt, sorting *godal.SortingOpt) ([]*UniversalBo, error) {
	return dao.GetN(0, 0, filter, sorting)
}

// Update implements UniversalDao.Update.
func (dao *UniversalDaoMongo) Update(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoUpdate(dao.collectionName, dao.ToGenericBo(bo))
	return numRows > 0, err
}

// Save implements UniversalDao.Save.
func (dao *UniversalDaoMongo) Save(bo *UniversalBo) (bool, *UniversalBo, error) {
	existing, err := dao.Get(bo.GetId())
	if err != nil {
		return false, nil, err
	}
	numRows, err := dao.GdaoSave(dao.collectionName, dao.ToGenericBo(bo))
	return numRows > 0, existing, err
}
