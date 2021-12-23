package henge

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/btnguyen2k/prom"
	_ "github.com/godror/godror"
)

func _testOracleInitSqlConnect(t *testing.T, testName string) *prom.SqlConnect {
	driver := strings.ReplaceAll(os.Getenv("ORACLE_DRIVER"), `"`, "")
	url := strings.ReplaceAll(os.Getenv("ORACLE_URL"), `"`, "")
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

	sqlc, err := NewOracleConnection(url, timezone, driver, 10000, nil)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewOracleConnection", err)
	}
	return sqlc
}

func TestInitOracleTable(t *testing.T) {
	testName := "TestInitOracleTable"
	teardownTest := setupTest(t, testName, setupTestOracle, teardownTestOracle)
	defer teardownTest(t)

	testSqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", testTable))
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitOracleTable(testSqlc, testTable, colDef); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

func TestCreateIndexOracle(t *testing.T) {
	testName := "TestCreateIndexOracle"
	teardownTest := setupTest(t, testName, setupTestOracle, teardownTestOracle)
	defer teardownTest(t)

	testSqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", testTable))
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitOracleTable(testSqlc, testTable, colDef); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if err := CreateIndexSql(testSqlc, testTable, true, []string{"col_email"}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	if err := CreateIndexSql(testSqlc, testTable, false, []string{"col_age"}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

var setupTestOracle = func(t *testing.T, testName string) {
	testSqlc = _testOracleInitSqlConnect(t, testName)
	testSqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", testTable))
	colDef := map[string]string{"col_email": "VARCHAR(64)", "col_age": "INT"}
	if err := InitOracleTable(testSqlc, testTable, colDef); err != nil {
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

var teardownTestOracle = func(t *testing.T, testName string) {
	if testSqlc != nil {
		defer func() { testSqlc, testDao = nil, nil }()
		testSqlc.Close()
	}
}
