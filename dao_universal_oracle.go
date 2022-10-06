package henge

import (
	prom "github.com/btnguyen2k/prom/sql"
)

// NewOracleConnection is helper function to create connection pools for Oracle.
//
// Note: it's application's responsibility to import proper Oracle driver, e.g. import _ "github.com/godror/godror"
// and supply the correct driver, e.g. "godror".
func NewOracleConnection(url, timezone, driver string, defaultTimeoutMs int, poolOptions *prom.SqlPoolOptions) (*prom.SqlConnect, error) {
	return NewSqlConnection(url, timezone, driver, prom.FlavorOracle, defaultTimeoutMs, poolOptions)
}

// InitOracleTable initializes a database table to store henge business objects.
//   - Table is created "if not exists" as the following: { SqlColId: "NVARCHAR2(64)", SqlColData: "CLOB",
//     SqlColChecksum: "NVARCHAR2(32)", SqlColTimeCreated: "TIMESTAMP WITH TIME ZONE", SqlColTimeUpdated: "TIMESTAMP WITH TIME ZONE",
//     SqlColTagVersion: "INT" }, plus additional column defined by extraCols parameter.
//   - SqlColId is table's primary key.
//   - extraCols (nillable) is a map of {col-name:col-type} and is supplied so that table columns other than
//     core columns are also created.
//   - extraCols can also be used to override data type of core columns.
//   - Other than the database table, no index is created.
func InitOracleTable(sqlc *prom.SqlConnect, tableName string, extraCols map[string]string) error {
	colDef := map[string]string{
		SqlColId:          "NVARCHAR2(64)",
		SqlColData:        "CLOB",
		SqlColChecksum:    "NVARCHAR2(32)",
		SqlColTimeCreated: "TIMESTAMP WITH TIME ZONE",
		SqlColTimeUpdated: "TIMESTAMP WITH TIME ZONE",
		SqlColTagVersion:  "INT",
	}
	colNames := sqlColumnNames
	for k, v := range extraCols {
		colDef[k] = v
		colNames = append(colNames, k)
	}
	pk := []string{SqlColId}
	return CreateTableSql(sqlc, tableName, false, colDef, colNames, pk)
}
