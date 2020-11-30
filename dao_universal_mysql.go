package henge

import (
	"time"

	"github.com/btnguyen2k/prom"
)

// NewMysqlConnection is helper function to create connection pools for MySQL.
//
// Note: it's application's responsibility to import proper MySQL driver, e.g. import _ "github.com/go-sql-driver/mysql"
// and supply the correct driver, e.g. "mysql".
func NewMysqlConnection(url, timezone, driver string, defaultTimeoutMs int, poolOptions *prom.SqlPoolOptions) (*prom.SqlConnect, error) {
	sqlc, err := prom.NewSqlConnect(driver, url, defaultTimeoutMs, poolOptions)
	if err != nil {
		return nil, err
	}
	loc, _ := time.LoadLocation(timezone)
	sqlc.SetLocation(loc).SetDbFlavor(prom.FlavorMySql)
	return sqlc, nil
}

// InitMysqlTable initializes a database table to store henge business objects.
//
// - Table is created "if not exists" as the following: { SqlColId: "VARCHAR(64)", SqlColData: "TEXT", SqlColChecksum: "VARCHAR(32)",
//     SqlColTimeCreated: "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", SqlColTimeUpdated: "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
//     SqlColTagVersion: "BIGINT" }, plus additional column defined by extraCols parameter.
// - SqlColId is table's primary key.
// - extraCols (nillable) is a map of {col-name:col-type} and is supplied so that table columns other than
//   core columns are also created.
// - extraCols can also be used to override data type of core columns.
// - Other than the database table, no index is created.
func InitMysqlTable(sqlc *prom.SqlConnect, tableName string, extraCols map[string]string) error {
	colDef := map[string]string{
		SqlColId:          "VARCHAR(64)",
		SqlColData:        "TEXT",
		SqlColChecksum:    "VARCHAR(32)",
		SqlColTimeCreated: "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
		SqlColTimeUpdated: "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
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
