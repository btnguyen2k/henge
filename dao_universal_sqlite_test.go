package henge

import (
	"fmt"
	"os"
	"strings"
	"testing"

	prom "github.com/btnguyen2k/prom/sql"
	_ "github.com/mattn/go-sqlite3"
)

func _testSqliteInitSqlConnect(t *testing.T, testName string) *prom.SqlConnect {
	driver := strings.ReplaceAll(os.Getenv("SQLITE_DRIVER"), `"`, "")
	url := strings.ReplaceAll(os.Getenv("SQLITE_URL"), `"`, "")
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

	dbName := "tempdb"
	sqlc, err := NewSqliteConnection(url, dbName, timezone, driver, 10000, nil)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewSqliteConnection", err)
	}
	return sqlc
}

func TestInitSqliteTable(t *testing.T) {
	testName := "TestInitSqliteTable"
	teardownTest := setupTest(t, testName, setupTestSqlite, teardownTestSqlite)
	defer teardownTest(t)

	testSqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", testTable))
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	for i := 0; i < 2; i++ {
		if err := InitSqliteTable(testSqlc, testTable, colDef); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
}

func TestCreateIndexSqlite(t *testing.T) {
	testName := "TestCreateIndexSqlite"
	teardownTest := setupTest(t, testName, setupTestSqlite, teardownTestSqlite)
	defer teardownTest(t)

	testSqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", testTable))
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitSqliteTable(testSqlc, testTable, colDef); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if err := CreateIndexSql(testSqlc, testTable, true, []string{"col_email"}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if err := CreateIndexSql(testSqlc, testTable, false, []string{"col_age"}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

var setupTestSqlite = func(t *testing.T, testName string) {
	testSqlc = _testSqliteInitSqlConnect(t, testName)
	testSqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", testTable))
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitSqliteTable(testSqlc, testTable, colDef); err != nil {
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

var teardownTestSqlite = func(t *testing.T, testName string) {
	if testSqlc != nil {
		defer func() { testSqlc, testDao = nil, nil }()
		testSqlc.Close()
	}
}
