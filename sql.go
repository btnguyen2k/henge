package henge

import (
	"fmt"
	"strings"

	prom "github.com/btnguyen2k/prom/sql"
)

// CreateTableSql generates and executes "CREATE TABLE" SQL statement.
//   - if ifNotExist is true the SQL statement will be generated as CREATE TABLE IF NOT EXISTS table-name...
//   - colDef is a map of {table-col-name:col-type} and must
func CreateTableSql(sqlc *prom.SqlConnect, tableName string, ifNotExist bool, colDef map[string]string, colNames, pk []string) error {
	template := "CREATE TABLE %s %s (%s%s)"
	partIfNotExists := ""
	if ifNotExist {
		partIfNotExists = "IF NOT EXISTS"
	}
	partColDef := make([]string, 0)
	for _, c := range colNames {
		partColDef = append(partColDef, c+" "+colDef[c])
	}
	partPk := strings.Join(pk, ",")
	if partPk != "" {
		partPk = ", PRIMARY KEY (" + partPk + ")"
	}
	sql := fmt.Sprintf(template, partIfNotExists, tableName, strings.Join(partColDef, ","), partPk)
	_, err := sqlc.GetDB().Exec(sql)
	return err
}

// CreateIndexSql generates and executes "CREATE INDEX" SQL statement.
func CreateIndexSql(sqlc *prom.SqlConnect, tableName string, unique bool, cols []string) error {
	template := "CREATE INDEX idx_%s_%s on %s(%s)"
	templateUnique := "CREATE UNIQUE INDEX udx_%s_%s on %s(%s)"
	var sql string
	if unique {
		sql = fmt.Sprintf(templateUnique, tableName, strings.Join(cols, "_"), tableName, strings.Join(cols, ","))
	} else {
		sql = fmt.Sprintf(template, tableName, strings.Join(cols, "_"), tableName, strings.Join(cols, ","))
	}
	_, err := sqlc.GetDB().Exec(sql)
	return err
}
