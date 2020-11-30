package henge

import (
	"time"

	"github.com/btnguyen2k/prom"
)

// NewMssqlConnection is helper function to create connection pools for MSSQL.
//
// Note: it's application's responsibility to import proper MSSQL driver, e.g. import _ "github.com/denisenkom/go-mssqldb"
// and supply the correct driver, e.g. "sqlserver"
func NewMssqlConnection(url, timezone, driver string, defaultTimeoutMs int, poolOptions *prom.SqlPoolOptions) (*prom.SqlConnect, error) {
	sqlc, err := prom.NewSqlConnect(driver, url, defaultTimeoutMs, poolOptions)
	if err != nil {
		return nil, err
	}
	loc, _ := time.LoadLocation(timezone)
	sqlc.SetLocation(loc).SetDbFlavor(prom.FlavorMsSql)
	return sqlc, nil
}

// InitMssqlTable initializes a database table to store henge business objects.
//
// - Table is created "if not exists" as the following: {SqlColId: "NVARCHAR(64)", SqlColData: "NTEXT",
//     SqlColChecksum: "NVARCHAR(32)", SqlColTimeCreated: "DATETIMEOFFSET", SqlColTimeUpdated: "DATETIMEOFFSET",
//     SqlColTagVersion: "BIGINT"}, plus additional column defined by extraCols parameter.
// - SqlColId is table's primary key.
func InitMssqlTable(sqlc *prom.SqlConnect, tableName string, extraCols map[string]string) error {
	colDef := map[string]string{
		SqlColId:          "NVARCHAR(64)",
		SqlColData:        "NTEXT",
		SqlColChecksum:    "NVARCHAR(32)",
		SqlColTimeCreated: "DATETIMEOFFSET",
		SqlColTimeUpdated: "DATETIMEOFFSET",
		SqlColTagVersion:  "BIGINT",
	}
	colNames := sqlColumnNames
	for k, v := range extraCols {
		colDef[k] = v
		colNames = append(colNames, k)
	}
	pk := []string{SqlColId}
	return CreateTableSql(sqlc, tableName, false, colDef, colNames, pk)
}
