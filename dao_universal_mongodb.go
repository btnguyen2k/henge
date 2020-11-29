package henge

import (
	"encoding/json"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/mongo"
	"github.com/btnguyen2k/prom"
)

// InitMongoCollection initializes a MongoDB collection to store henge business objects.
func InitMongoCollection(mc *prom.MongoConnect, collectionName string) error {
	_, err := mc.CreateCollection(collectionName)
	return err
}

func buildRowMapperMongo() godal.IRowMapper {
	return &rowMapperMongo{wrap: mongo.GenericRowMapperMongoInstance}
}

// rowMapperMongo is an implementation of godal.IRowMapper specific for MongoDB.
type rowMapperMongo struct {
	wrap godal.IRowMapper
}

// ToRow implements godal.IRowMapper.ToRow
func (r *rowMapperMongo) ToRow(storageId string, bo godal.IGenericBo) (interface{}, error) {
	row, err := r.wrap.ToRow(storageId, bo)
	if m, ok := row.(map[string]interface{}); err == nil && ok {
		m[MongoColId] = m[FieldId]
		delete(m, FieldId)
		m[FieldTagVersion], _ = bo.GboGetAttr(FieldTagVersion, nil) // tag-version should be integer
		m[FieldTimeCreated], _ = bo.GboGetTimeWithLayout(FieldTimeCreated, TimeLayout)
		m[FieldTimeUpdated], _ = bo.GboGetTimeWithLayout(FieldTimeUpdated, TimeLayout)
		m[FieldData], _ = bo.GboGetAttrUnmarshalJson(FieldData)
	}
	return row, nil
}

// ToBo implements godal.IRowMapper.ToBo
func (r *rowMapperMongo) ToBo(storageId string, row interface{}) (godal.IGenericBo, error) {
	gbo, err := r.wrap.ToBo(storageId, row)
	if err == nil {
		var v interface{}
		v, err = gbo.GboGetAttr(MongoColId, nil)
		gbo.GboSetAttr(MongoColId, nil)
		gbo.GboSetAttr(FieldId, v)
		if data, err := gbo.GboGetAttr(FieldData, nil); err == nil {
			js, _ := json.Marshal(data)
			gbo.GboSetAttr(FieldData, string(js))
		}
	}
	return gbo, err
}

// ColumnsList implements godal.IRowMapper.ColumnsList
func (r *rowMapperMongo) ColumnsList(storageId string) []string {
	return r.wrap.ColumnsList(storageId)
}

// NewUniversalDaoMongo is helper method to create UniversalDaoMongo instance.
//
// - txModeOnWrite: enables/disables transaction mode on write operations.
//       MongoDB's implementation of GdaoCreate is "get/check and write".
//       It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.
//       As of MongoDB 4.0, transactions are available for replica set deployments only. Since MongoDB 4.2, transactions are also available for sharded cluster.
//       It is recommended to set "txModeOnWrite=true" whenever possible.
func NewUniversalDaoMongo(mc *prom.MongoConnect, collectionName string, txModeOnWrite bool) UniversalDao {
	dao := &UniversalDaoMongo{collectionName: collectionName}
	dao.GenericDaoMongo = mongo.NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(buildRowMapperMongo())
	dao.SetTxModeOnWrite(txModeOnWrite)
	return dao
}

const (
	// MongoColId holds the name of MongoDB collection's "id" field.
	MongoColId = "_id"
)

// UniversalDaoMongo is MongoDB-based implementation of UniversalDao.
type UniversalDaoMongo struct {
	*mongo.GenericDaoMongo
	collectionName string
}

// GdaoCreateFilter implements IGenericDao.GdaoCreateFilter.
func (dao *UniversalDaoMongo) GdaoCreateFilter(_ string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{MongoColId: bo.GboGetAttrUnsafe(FieldId, reddo.TypeString)}
}

// ToUniversalBo transforms godal.IGenericBo to business object.
func (dao *UniversalDaoMongo) ToUniversalBo(gbo godal.IGenericBo) *UniversalBo {
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
func (dao *UniversalDaoMongo) ToGenericBo(ubo *UniversalBo) godal.IGenericBo {
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
func (dao *UniversalDaoMongo) Delete(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoDelete(dao.collectionName, dao.ToGenericBo(bo))
	return numRows > 0, err
}

// Create implements UniversalDao.Create.
func (dao *UniversalDaoMongo) Create(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoCreate(dao.collectionName, dao.ToGenericBo(bo.Clone()))
	return numRows > 0, err
}

// Get implements UniversalDao.Get.
func (dao *UniversalDaoMongo) Get(id string) (*UniversalBo, error) {
	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{MongoColId: id})
	if err != nil {
		return nil, err
	}
	return dao.ToUniversalBo(gbo), nil
}

// GetN implements UniversalDao.GetN.
func (dao *UniversalDaoMongo) GetN(fromOffset, maxNumRows int, filter interface{}, sorting interface{}) ([]*UniversalBo, error) {
	if sorting == nil {
		// default sorting: ascending by "id" column
		sorting = map[string]int{MongoColId: 1}
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
func (dao *UniversalDaoMongo) GetAll(filter interface{}, sorting interface{}) ([]*UniversalBo, error) {
	return dao.GetN(0, 0, filter, sorting)
}

// Update implements UniversalDao.Update.
func (dao *UniversalDaoMongo) Update(bo *UniversalBo) (bool, error) {
	numRows, err := dao.GdaoUpdate(dao.collectionName, dao.ToGenericBo(bo.Clone()))
	return numRows > 0, err
}

// Save implements UniversalDao.Save.
func (dao *UniversalDaoMongo) Save(bo *UniversalBo) (bool, *UniversalBo, error) {
	existing, err := dao.Get(bo.GetId())
	if err != nil {
		return false, nil, err
	}
	numRows, err := dao.GdaoSave(dao.collectionName, dao.ToGenericBo(bo.Clone()))
	return numRows > 0, existing, err
}
