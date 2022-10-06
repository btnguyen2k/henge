package henge

import (
	"testing"

	"github.com/btnguyen2k/prom/dynamodb"
	"github.com/btnguyen2k/prom/mongo"
	"github.com/btnguyen2k/prom/sql"
)

type TestSetupOrTeardownFunc func(t *testing.T, testName string)

func setupTest(t *testing.T, testName string, extraSetupFunc, extraTeardownFunc TestSetupOrTeardownFunc) func(t *testing.T) {
	if extraSetupFunc != nil {
		extraSetupFunc(t, testName)
	}
	return func(t *testing.T) {
		if extraTeardownFunc != nil {
			extraTeardownFunc(t, testName)
		}
	}
}

var (
	testAdc  *dynamodb.AwsDynamodbConnect
	testMc   *mongo.MongoConnect
	testSqlc *sql.SqlConnect
	testDao  UniversalDao
)

const (
	testTable = "table_temp"
)
