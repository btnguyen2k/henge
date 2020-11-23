package henge

import (
	"time"

	"github.com/btnguyen2k/prom"
)

// NewPgsqlConnection is helper function to create connection pools for PostgreSQL.
//
// Note: it's application's responsibility to import proper PostgreSQL driver, e.g. import _ "github.com/jackc/pgx/v4/stdlib"
// and supply the correct driver, e.g. "pgx"
func NewPgsqlConnection(url, timezone, driver string, defaultTimeoutMs int, poolOptions *prom.SqlPoolOptions) (*prom.SqlConnect, error) {
	sqlc, err := prom.NewSqlConnect(driver, url, defaultTimeoutMs, poolOptions)
	if err != nil {
		return nil, err
	}
	loc, _ := time.LoadLocation(timezone)
	sqlc.SetLocation(loc).SetDbFlavor(prom.FlavorPgSql)
	return sqlc, nil
}

// InitPgsqlTable initializes a database table to store henge business objects.
//
// - Table is created "if not exists" as the following: {SqlColId: "VARCHAR(64)", SqlColData: "JSONB", SqlColChecksum: "VARCHAR(32)",
//     SqlColTimeCreated: "TIMESTAMP WITH TIME ZONE", SqlColTimeUpdated: "TIMESTAMP WITH TIME ZONE",
//     SqlColTagVersion: "BIGINT"}, plus additional column defined by extraCols parameter.
// - SqlColId is table's primary key.
func InitPgsqlTable(sqlc *prom.SqlConnect, tableName string, extraCols map[string]string) error {
	colDef := map[string]string{
		SqlColId:          "VARCHAR(64)",
		SqlColData:        "JSONB",
		SqlColChecksum:    "VARCHAR(32)",
		SqlColTimeCreated: "TIMESTAMP WITH TIME ZONE",
		SqlColTimeUpdated: "TIMESTAMP WITH TIME ZONE",
		SqlColTagVersion:  "BIGINT",
	}
	colNames := sqlColumnNames
	for k, v := range extraCols {
		colDef[k] = v
		colNames = append(colNames, k)
	}
	pk := []string{SqlColId}
	return CreateTableSql(sqlc, tableName, true, colDef, colNames, pk)
}
