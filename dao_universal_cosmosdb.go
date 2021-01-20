package henge

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/cosmosdbsql"
	"github.com/btnguyen2k/godal/sql"
	"github.com/btnguyen2k/prom"
)

// InitCosmosdbCollection initializes a database collection to store henge business objects.
//   - Collection is created with "IF NOT EXISTS".
//   - pkName is name of collection's partition key.
//   - unique keys are specified as ukList.
//
// Available: since v0.3.0
func InitCosmosdbCollection(sqlc *prom.SqlConnect, tableName, pkName string, ru, maxru int, ukList [][]string) error {
	return CreateCollectionCosmosdb(sqlc, tableName, true, pkName, "", ru, maxru, ukList)
}

// CreateCollectionCosmosdb generates and executes "CREATE COLLECTION" SQL statement.
//   - if ifNotExist is true the SQL statement will be generated as "CREATE COLLECTION IF NOT EXISTS collection-name...".
//   - specify only one pkName or largePkName for name of collection's partition key.
//   - unique keys are specified as ukList.
//
// Available: since v0.3.0
func CreateCollectionCosmosdb(sqlc *prom.SqlConnect, tableName string, ifNotExist bool, pkName, largePkName string, ru, maxru int, ukList [][]string) error {
	template := "CREATE COLLECTION %s %s WITH %s"
	partIfNotExists := ""
	if ifNotExist {
		partIfNotExists = "IF NOT EXISTS"
	}
	partPk := "pk=/" + strings.TrimSpace(pkName)
	if strings.TrimSpace(largePkName) != "" {
		partPk = "largepk=/" + strings.TrimSpace(largePkName)
	}
	sql := fmt.Sprintf(template, partIfNotExists, tableName, partPk)
	if ru > 0 {
		sql += " WITH ru=" + strconv.Itoa(ru)
	}
	if maxru > 0 {
		sql += " WITH maxru=" + strconv.Itoa(maxru)
	}
	partUk := ""
	if len(ukList) > 0 {
		for i, uk := range ukList {
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
	CosmosdbColId = "id"
)

func buildRowMapperCosmosdb() godal.IRowMapper {
	return &rowMapperCosmosdb{wrap: cosmosdbsql.GenericRowMapperCosmosdbInstance}
}

// rowMapperCosmosdb is an implementation of godal.IRowMapper specific for Azure Cosmos DB.
type rowMapperCosmosdb struct {
	wrap godal.IRowMapper
}

// ToRow implements godal.IRowMapper.ToRow.
func (r *rowMapperCosmosdb) ToRow(storageId string, bo godal.IGenericBo) (interface{}, error) {
	row, err := r.wrap.ToRow(storageId, bo)
	if m, ok := row.(map[string]interface{}); err == nil && ok && m != nil {
		m[CosmosdbColId] = m[FieldId]
		if CosmosdbColId != FieldId {
			delete(m, FieldId)
		}
		m[FieldTagVersion], _ = bo.GboGetAttr(FieldTagVersion, nil) // tag-version should be integer
		m[FieldTimeCreated], _ = bo.GboGetTimeWithLayout(FieldTimeCreated, TimeLayout)
		m[FieldTimeUpdated], _ = bo.GboGetTimeWithLayout(FieldTimeUpdated, TimeLayout)
		m[FieldData], _ = bo.GboGetAttrUnmarshalJson(FieldData) // Note: FieldData must be JSON-encoded string!
	}
	return row, err
}

// ToBo implements godal.IRowMapper.ToBo.
func (r *rowMapperCosmosdb) ToBo(storageId string, row interface{}) (godal.IGenericBo, error) {
	gbo, err := r.wrap.ToBo(storageId, row)
	if err == nil && gbo != nil {
		var v interface{}
		v, err = gbo.GboGetAttr(CosmosdbColId, nil)
		gbo.GboSetAttr(CosmosdbColId, nil)
		gbo.GboSetAttr(FieldId, v)
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
func (r *rowMapperCosmosdb) ColumnsList(storageId string) []string {
	return r.wrap.ColumnsList(storageId)
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

// NewUniversalDaoCosmosdbSql is helper method to create UniversalDaoSql instance specific for Azure Cosmos DB.
//   - txModeOnWrite: added for compatibility,not used.
//
// Available: since v0.3.0
func NewUniversalDaoCosmosdbSql(sqlc *prom.SqlConnect, tableName, pkName string, txModeOnWrite bool) UniversalDao {
	dao := &UniversalDaoSql{tableName: tableName}
	// dao := &UniversalDaoCosmosdbSql{}
	// dao.tableName = tableName
	inner := cosmosdbsql.NewGenericDaoCosmosdb(sqlc, godal.NewAbstractGenericDao(dao))
	// it's recommend to provide {collection-name:path-to-fetch-id-value-from-genericbo} for performance reason
	inner.CosmosSetIdGboMapPath(map[string]string{tableName: FieldId})
	// at least one of {collection-name:path-to-fetch-partition_key-value-from-genericbo} or
	// {collection-name:path-to-fetch-partition_key-value-from-dbrow} must be configured
	inner.CosmosSetPkRowMapPath(map[string]string{tableName: pkName})
	dao.IGenericDaoSql = inner
	dao.SetRowMapper(buildRowMapperCosmosdb())
	dao.SetTxModeOnWrite(txModeOnWrite).SetSqlFlavor(sqlc.GetDbFlavor())
	dao.funcFilterGeneratorSql = cosmosdbFilterGeneratorSql
	dao.defaultSorting = (&sql.GenericSorting{Flavor: sqlc.GetDbFlavor()}).Add(CosmosdbColId)
	return dao
}

// cosmosdbFilterGeneratorSql is CosmosDB-implementation of FuncFilterGeneratorSql.
func cosmosdbFilterGeneratorSql(_ string, input interface{}) interface{} {
	switch input.(type) {
	case UniversalBo:
		bo := input.(UniversalBo)
		return map[string]interface{}{CosmosdbColId: bo.id}
	case *UniversalBo:
		bo := input.(*UniversalBo)
		return map[string]interface{}{CosmosdbColId: bo.id}
	}
	if gbo, ok := input.(godal.IGenericBo); ok {
		return map[string]interface{}{CosmosdbColId: gbo.GboGetAttrUnsafe(FieldId, reddo.TypeString)}
	}
	return input
}
