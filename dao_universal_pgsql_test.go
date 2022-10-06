package henge

import (
	"fmt"
	"os"
	"strings"
	"testing"

	prom "github.com/btnguyen2k/prom/sql"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func _testPgsqlInitSqlConnect(t *testing.T, testName string) *prom.SqlConnect {
	driver := strings.ReplaceAll(os.Getenv("PGSQL_DRIVER"), `"`, "")
	url := strings.ReplaceAll(os.Getenv("PGSQL_URL"), `"`, "")
	if driver == "" || url == "" {
		t.Skipf("%s skipped", testName)
		return nil
	}
	timezone := strings.ReplaceAll(os.Getenv("TIMEZONE"), `"`, "")
	if timezone == "" {
		timezone = "Asia/Ho_Chi_Minh"
	}
	urlTimezone := strings.ReplaceAll(timezone, "/", "%2f")
	url = strings.ReplaceAll(url, "${loc}", urlTimezone)
	url = strings.ReplaceAll(url, "${tz}", urlTimezone)
	url = strings.ReplaceAll(url, "${timezone}", urlTimezone)

	sqlc, err := NewPgsqlConnection(url, timezone, driver, 10000, nil)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewPgsqlConnection", err)
	}
	return sqlc
}

func TestInitPgsqlTable(t *testing.T) {
	testName := "TestInitPgsqlTable"
	teardownTest := setupTest(t, testName, setupTestPgsql, teardownTestPgsql)
	defer teardownTest(t)

	testSqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	for i := 0; i < 2; i++ {
		if err := InitPgsqlTable(testSqlc, testTable, colDef); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
}

func TestCreateIndexPgsql(t *testing.T) {
	testName := "TestCreateIndexPgsql"
	teardownTest := setupTest(t, testName, setupTestPgsql, teardownTestPgsql)
	defer teardownTest(t)

	testSqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitPgsqlTable(testSqlc, testTable, colDef); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if err := CreateIndexSql(testSqlc, testTable, true, []string{"col_email"}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if err := CreateIndexSql(testSqlc, testTable, false, []string{"col_age"}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

var setupTestPgsql = func(t *testing.T, testName string) {
	testSqlc = _testPgsqlInitSqlConnect(t, testName)
	testSqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", testTable))
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitPgsqlTable(testSqlc, testTable, colDef); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if err := CreateIndexSql(testSqlc, testTable, true, []string{"col_email"}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if err := CreateIndexSql(testSqlc, testTable, false, []string{"col_age"}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	extraColNameToFieldMappings := map[string]string{"col_email": "email", "col_age": "age"}
	testDao = NewUniversalDaoSql(testSqlc, testTable, true, extraColNameToFieldMappings)
}

var teardownTestPgsql = func(t *testing.T, testName string) {
	if testSqlc != nil {
		defer func() { testSqlc, testDao = nil, nil }()
		testSqlc.Close()
	}
}
