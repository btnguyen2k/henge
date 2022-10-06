package henge

import (
	prom "github.com/btnguyen2k/prom/sql"
)

// NewMssqlConnection is helper function to create connection pools for MSSQL.
//
// Note: it's application's responsibility to import proper MSSQL driver, e.g. import _ "github.com/denisenkom/go-mssqldb"
// and supply the correct driver, e.g. "sqlserver".
func NewMssqlConnection(url, timezone, driver string, defaultTimeoutMs int, poolOptions *prom.SqlPoolOptions) (*prom.SqlConnect, error) {
	return NewSqlConnection(url, timezone, driver, prom.FlavorMsSql, defaultTimeoutMs, poolOptions)
}

// InitMssqlTable initializes a database table to store henge business objects.
//   - Table is created as the following: { SqlColId: "NVARCHAR(64)", SqlColData: "NTEXT",
//     SqlColChecksum: "NVARCHAR(32)", SqlColTimeCreated: "DATETIMEOFFSET", SqlColTimeUpdated: "DATETIMEOFFSET",
//     SqlColTagVersion: "BIGINT" }, plus additional column defined by extraCols parameter.
//   - This function returns error if the table already existed.
//   - SqlColId is table's primary key.
//   - extraCols (nillable) is a map of {col-name:col-type} and is supplied so that table columns other than
//     core columns are also created.
//   - extraCols can also be used to override data type of core columns.
//   - Other than the database table, no index is created.
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
