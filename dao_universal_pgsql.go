package henge

import (
	"github.com/btnguyen2k/prom"
)

// NewPgsqlConnection is helper function to create connection pools for PostgreSQL.
//
// Note: it's application's responsibility to import proper PostgreSQL driver, e.g. import _ "github.com/jackc/pgx/v4/stdlib"
// and supply the correct driver, e.g. "pgx".
func NewPgsqlConnection(url, timezone, driver string, defaultTimeoutMs int, poolOptions *prom.SqlPoolOptions) (*prom.SqlConnect, error) {
	return NewSqlConnection(url, timezone, driver, prom.FlavorPgSql, defaultTimeoutMs, poolOptions)
}

// InitPgsqlTable initializes a database table to store henge business objects.
//   - Table is created "if not exists" as the following: { SqlColId: "VARCHAR(64)", SqlColData: "JSONB", SqlColChecksum: "VARCHAR(32)",
//     SqlColTimeCreated: "TIMESTAMP WITH TIME ZONE", SqlColTimeUpdated: "TIMESTAMP WITH TIME ZONE",
//     SqlColTagVersion: "BIGINT" }, plus additional column defined by extraCols parameter.
//   - SqlColId is table's primary key.
//   - extraCols (nillable) is a map of {col-name:col-type} and is supplied so that table columns other than
//     core columns are also created.
//   - extraCols can also be used to override data type of core columns.
//   - Other than the database table, no index is created.
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
