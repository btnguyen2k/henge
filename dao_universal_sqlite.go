package henge

import (
	"os"

	"github.com/btnguyen2k/prom"
)

// NewSqliteConnection is helper function to create connection pools for SQLite.
//
// - dir is the root directory to store SQLite data files.
// - dbName is name of the SQLite database.
//
// Note: it's application's responsibility to import proper sqlite driver, e.g. import _ "github.com/mattn/go-sqlite3"
// and supply the correct driver, e.g. "sqlite3"
func NewSqliteConnection(dir, dbName, driver string, defaultTimeoutMs int, poolOptions *prom.SqlPoolOptions) (*prom.SqlConnect, error) {
	err := os.MkdirAll(dir, 0711)
	if err != nil {
		return nil, err
	}
	sqlc, err := prom.NewSqlConnect(driver, dir+"/"+dbName+".db", defaultTimeoutMs, poolOptions)
	if err != nil {
		return nil, err
	}
	sqlc.SetDbFlavor(prom.FlavorSqlite)
	return sqlc, nil
}

// InitSqliteTable initializes a database table to store henge business objects.
//
// - Table is created "if not exists" as the following: {SqlColId: "VARCHAR(64)", SqlColData: "TEXT",
//     SqlColChecksum: "VARCHAR(32)", SqlColTimeCreated: "TIMESTAMP", SqlColTimeUpdated: "TIMESTAMP",
//     SqlColTagVersion: "BIGINT"}, plus additional column defined by extraCols parameter.
// - SqlColId is table's primary key.
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
