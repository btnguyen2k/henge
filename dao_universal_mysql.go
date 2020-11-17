package henge

import (
	"time"

	"github.com/btnguyen2k/prom"
)

// NewMysqlConnection creates a new connection pool for MySQL database.
//
// Note: it's application's responsibility to import proper MySQL driver, e.g. import _ "github.com/go-sql-driver/mysql"
// and supply the correct driver, e.g. "mysql"
func NewMysqlConnection(url, timezone, driver string, defaultTimeoutMs int, poolOptions *prom.SqlPoolOptions) (*prom.SqlConnect, error) {
	sqlc, err := prom.NewSqlConnect(driver, url, defaultTimeoutMs, poolOptions)
	if err != nil {
		return nil, err
	}
	loc, _ := time.LoadLocation(timezone)
	sqlc.SetLocation(loc).SetDbFlavor(prom.FlavorMySql)
	return sqlc, nil
}

// InitMysqlTable initializes a database table.
//
// - Table is created "if not exists" as the following: {SqlColId: "VARCHAR(64)", SqlColData: "TEXT", SqlColChecksum: "VARCHAR(32)",
//     SqlColTimeCreated: "TIMESTAMP DEFAULT CURRENT_TIMESTAMP", SqlColTimeUpdated: "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
//     SqlColTagVersion: "BIGINT"}, plus additional column defined by extraCols parameter.
// - SqlColId is table's primary key.
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
