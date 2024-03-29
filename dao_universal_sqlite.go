package henge

import (
	"os"

	prom "github.com/btnguyen2k/prom/sql"
)

// NewSqliteConnection is helper function to create connection pools for SQLite.
//   - dir is the root directory to store SQLite data files.
//   - dbName is name of the SQLite database.
//
// Note: it's application's responsibility to import proper sqlite driver, e.g. import _ "github.com/mattn/go-sqlite3"
// and supply the correct driver, e.g. "sqlite3".
func NewSqliteConnection(dir, dbName, timezone, driver string, defaultTimeoutMs int, poolOptions *prom.SqlPoolOptions) (*prom.SqlConnect, error) {
	err := os.MkdirAll(dir, 0711)
	if err != nil {
		return nil, err
	}
	return NewSqlConnection(dir+"/"+dbName+".db", timezone, driver, prom.FlavorSqlite, defaultTimeoutMs, poolOptions)
}

// InitSqliteTable initializes a database table to store henge business objects.
//   - Table is created "if not exists" as the following: { SqlColId: "VARCHAR(64)", SqlColData: "TEXT",
//     SqlColChecksum: "VARCHAR(32)", SqlColTimeCreated: "TIMESTAMP", SqlColTimeUpdated: "TIMESTAMP",
//     SqlColTagVersion: "BIGINT" }, plus additional column defined by extraCols parameter.
//   - SqlColId is table's primary key.
//   - extraCols (nillable) is a map of {col-name:col-type} and is supplied so that table columns other than
//     core columns are also created.
//   - extraCols can also be used to override data type of core columns.
//   - Other than the database table, no index is created.
func InitSqliteTable(sqlc *prom.SqlConnect, tableName string, extraCols map[string]string) error {
	colDef := map[string]string{
		SqlColId:          "VARCHAR(64)",
		SqlColData:        "TEXT",
		SqlColChecksum:    "VARCHAR(32)",
		SqlColTimeCreated: "TIMESTAMP",
		SqlColTimeUpdated: "TIMESTAMP",
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
