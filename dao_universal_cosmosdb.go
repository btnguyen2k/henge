package henge

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/cosmosdbsql"
	"github.com/btnguyen2k/prom"
)

// CosmosdbCollectionSpec holds specification of CosmosDB collection to be created.
//
// Available: since v0.3.2
type CosmosdbCollectionSpec struct {
	Ru, MaxRu   int        // collection's RU/MAXRU setting
	Pk, LargePk string     // collection's PK or Large-PK name
	Uk          [][]string // collection's unique key settings
}

// InitCosmosdbCollection initializes a database collection to store henge business objects.
//
// Collection is created with "IF NOT EXISTS".
//
// Available: since v0.3.2
func InitCosmosdbCollection(sqlc *prom.SqlConnect, tableName string, spec *CosmosdbCollectionSpec) error {
	template := "CREATE COLLECTION IF NOT EXISTS %s WITH %s"
	partPk := "pk=/" + spec.Pk
	if spec.LargePk != "" {
		partPk = "largepk=/" + spec.LargePk
	}
	sql := fmt.Sprintf(template, tableName, partPk)
	if spec.Ru > 0 {
		sql += " WITH ru=" + strconv.Itoa(spec.Ru)
	}
	if spec.MaxRu > 0 {
		sql += " WITH maxru=" + strconv.Itoa(spec.MaxRu)
	}
	partUk := ""
	if len(spec.Uk) > 0 {
		for i, uk := range spec.Uk {
			if i > 0 {
				partUk += ":"
			}
			partUk += strings.Join(uk, ",")
		}
	}
	if partUk != "" {
		sql += " WITH uk=" + partUk
	}
	_, err := sqlc.GetDB().Exec(sql)
	return err
}

/*----------------------------------------------------------------------*/

const (
	// CosmosdbColId holds the name of CosmosDB collection's "id" field.
	CosmosdbColId = FieldId // since CosmosDB is schemaless, name of "id" field can be the same as name ò "id" db column
)

func buildRowMapperCosmosdb() godal.IRowMapper {
	return &rowMapperCosmosdb{cosmosdbsql.GenericRowMapperCosmosdbInstance}
}

// rowMapperCosmosdb is an implementation of godal.IRowMapper specific for Azure Cosmos DB.
type rowMapperCosmosdb struct {
	godal.IRowMapper
}

// ToRow implements godal.IRowMapper.ToRow.
func (r *rowMapperCosmosdb) ToRow(storageId string, bo godal.IGenericBo) (interface{}, error) {
	row, err := r.IRowMapper.ToRow(storageId, bo)
	if m, ok := row.(map[string]interface{}); err == nil && ok && m != nil {
		// if CosmosdbColId != FieldId {
		// 	m[CosmosdbColId] = m[FieldId]
		// 	delete(m, FieldId)
		// }
		m[FieldTagVersion], _ = bo.GboGetAttr(FieldTagVersion, nil) // tag-version should be integer
		m[FieldTimeCreated], _ = bo.GboGetTimeWithLayout(FieldTimeCreated, time.RFC3339)
		m[FieldTimeUpdated], _ = bo.GboGetTimeWithLayout(FieldTimeUpdated, time.RFC3339)
		m[FieldData], _ = bo.GboGetAttrUnmarshalJson(FieldData) // Note: FieldData must be JSON-encoded string!
	}
	return row, err
}

// ToBo implements godal.IRowMapper.ToBo.
func (r *rowMapperCosmosdb) ToBo(storageId string, row interface{}) (godal.IGenericBo, error) {
	gbo, err := r.IRowMapper.ToBo(storageId, row)
	if err == nil && gbo != nil {
		// if CosmosdbColId != FieldId {
		// 	v, _ := gbo.GboGetAttr(CosmosdbColId, nil)
		// 	gbo.GboSetAttr(CosmosdbColId, nil)
		// 	gbo.GboSetAttr(FieldId, v)
		// }
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

// NewCosmosdbConnection is helper function to create connection pools for Azure Cosmos DB using database/sql interface.
//
// Note: it's application's responsibility to import proper Azure Cosmos DB driver, e.g. import _ "github.com/btnguyen2k/gocosmos"
// and supply the correct driver, e.g. "gocosmos".
//
// Available: since v0.3.0
func NewCosmosdbConnection(url, timezone, driver string, defaultTimeoutMs int, poolOptions *prom.SqlPoolOptions) (*prom.SqlConnect, error) {
	return NewSqlConnection(url, timezone, driver, prom.FlavorCosmosDb, defaultTimeoutMs, poolOptions)
}

// CosmosdbDaoSpec holds specification of UniversalDaoCosmosdb to be created.
//
// Available: since v0.3.2
type CosmosdbDaoSpec struct {
	PkName        string // (multi-tenant) name of collection's PK attribute
	PkValue       string // (multi-tenant) static value for PkName attribute
	TxModeOnWrite bool   // for compatibility only, not used.
}

// NewUniversalDaoCosmosdbSql is helper method to create UniversalDaoSql instance specific for Azure Cosmos DB.
//   - txModeOnWrite: added for compatibility,not used.
//
// Available: since v0.3.0
func NewUniversalDaoCosmosdbSql(sqlc *prom.SqlConnect, tableName string, spec *CosmosdbDaoSpec) UniversalDao {
	if spec == nil {
		return nil
	}
	spec = &(*spec)
	dao := &UniversalDaoCosmosdbSql{
		UniversalDaoSql: &UniversalDaoSql{tableName: tableName},
		pkName:          spec.PkName,
		pkValue:         spec.PkValue,
	}
	inner := cosmosdbsql.NewGenericDaoCosmosdb(sqlc, godal.NewAbstractGenericDao(dao))
	// it's recommend to provide {collection-name:path-to-fetch-id-value-from-genericbo} for performance reason
	inner.CosmosSetIdGboMapPath(map[string]string{tableName: FieldId})
	// at least one of {collection-name:path-to-fetch-partition_key-value-from-genericbo} or
	// {collection-name:path-to-fetch-partition_key-value-from-dbrow} must be configured
	inner.CosmosSetPkRowMapPath(map[string]string{tableName: spec.PkName})
	dao.IGenericDaoSql = inner
	dao.SetRowMapper(buildRowMapperCosmosdb())
	dao.SetTxModeOnWrite(spec.TxModeOnWrite).SetSqlFlavor(sqlc.GetDbFlavor())
	dao.funcFilterGeneratorSql = cosmosdbFilterGeneratorSql
	dao.defaultSorting = (&godal.SortingField{FieldName: CosmosdbColId}).ToSortingOpt()

	return dao
}

// cosmosdbFilterGeneratorSql is CosmosDB-implementation of FuncFilterGeneratorSql.
func cosmosdbFilterGeneratorSql(_ string, input interface{}) godal.FilterOpt {
	switch input.(type) {
	case UniversalBo:
		bo := input.(UniversalBo)
		return godal.MakeFilter(map[string]interface{}{CosmosdbColId: bo.id})
	case *UniversalBo:
		bo := input.(*UniversalBo)
		return godal.MakeFilter(map[string]interface{}{CosmosdbColId: bo.id})
	}
	if gbo, ok := input.(godal.IGenericBo); ok && gbo != nil {
		return godal.MakeFilter(map[string]interface{}{CosmosdbColId: gbo.GboGetAttrUnsafe(FieldId, reddo.TypeString)})
	}
	if filter, ok := input.(godal.FilterOpt); ok {
		return filter
	}
	return nil
}

// UniversalDaoCosmosdbSql is CosmosDB-based (using driver/sql interface) implementation of UniversalDao.
//
// Available: since v0.3.2
type UniversalDaoCosmosdbSql struct {
	*UniversalDaoSql
	pkName, pkValue string // attribute name and static value of collection's PK
}

// GetPkName returns attribute name of collection's PK.
func (dao *UniversalDaoCosmosdbSql) GetPkName() string {
	return dao.pkName
}

// GetPkValue returns static value of collection's PK.
func (dao *UniversalDaoCosmosdbSql) GetPkValue() string {
	return dao.pkValue
}

var cosmosdbFields = []string{"_attachments", "_etag", "_rid", "_self", "_ts"}

// ToUniversalBo transforms godal.IGenericBo to business object.
func (dao *UniversalDaoCosmosdbSql) ToUniversalBo(gbo godal.IGenericBo) *UniversalBo {
	if gbo != nil {
		// remove CosmosDB's specific fields
		for _, field := range cosmosdbFields {
			gbo.GboSetAttr(field, nil)
		}
	}
	return dao.UniversalDaoSql.ToUniversalBo(gbo)
}

// Get implements UniversalDao.Get.
func (dao *UniversalDaoCosmosdbSql) Get(id string) (*UniversalBo, error) {
	filter := map[string]interface{}{CosmosdbColId: id}
	if dao.pkName != "" && dao.pkValue != "" {
		filter[dao.pkName] = dao.pkValue
	}
	gbo, err := dao.GdaoFetchOne(dao.tableName, godal.MakeFilter(filter))
	if err != nil {
		return nil, err
	}
	return dao.ToUniversalBo(gbo), nil
}

// GetN implements UniversalDao.GetN.
func (dao *UniversalDaoCosmosdbSql) GetN(fromOffset, maxNumRows int, filter godal.FilterOpt, sorting *godal.SortingOpt) ([]*UniversalBo, error) {
	if sorting == nil {
		sorting = dao.defaultSorting
	}
	if dao.pkName != "" && dao.pkValue != "" {
		/* multi-tenant: add tenant filtering */
		tempFilter := &godal.FilterOptAnd{}
		if filter != nil {
			tempFilter.Add(filter)
		}
		tempFilter.Add(&godal.FilterOptFieldOpValue{FieldName: dao.pkName, Operator: godal.FilterOpEqual, Value: dao.pkValue})
		filter = tempFilter
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
func (dao *UniversalDaoCosmosdbSql) GetAll(filter godal.FilterOpt, sorting *godal.SortingOpt) ([]*UniversalBo, error) {
	return dao.GetN(0, 0, filter, sorting)
}

// Save implements UniversalDao.Save.
func (dao *UniversalDaoCosmosdbSql) Save(bo *UniversalBo) (bool, *UniversalBo, error) {
	existing, err := dao.Get(bo.GetId())
	if err != nil {
		return false, nil, err
	}
	numRows, err := dao.GdaoSave(dao.tableName, dao.ToGenericBo(bo))
	return numRows > 0, existing, err
}
