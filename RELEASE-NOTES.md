# henge release notes

## 2021-02-06 - v0.3.2

- `UniversalDaoDynamodb`: add `pkPrefix` and `pkPrefixValue`, supporting multi-tenant DynamoDB tables.
- `UniversalDaoCosmosdbSql`: support multi-tenant CosmosDB collections.
- **Breaking changes**:
  - New structs `DynamodbTablesSpec` and `DynamodbDaoSpec`, signatures of functions `InitDynamodbTables` and `NewUniversalDaoDynamodb` changed.
  - New structs `CosmosdbCollectionSpec` and `CosmosdbDaoSpec`, signatures of functions `InitCosmosdbCollection` and `NewUniversalDaoCosmosdbSql` changed.

## 2021-01-20 - v0.3.1

- Function `InitDynamodbTable` is deprecated, use `InitDynamodbTables` instead.
- Add support for [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/introduction) using [btnguyen2k/gocosmos](https://github.com/btnguyen2k/gocosmos) driver.
- Other fixes & enhancements.

## 2020-12-02 - v0.2.2

- Store `zdata` field to DynamoDB as object.
- Other fixes & enhancements.

## 2020-11-30 - v0.2.1

- Fix module name.

## 2020-11-29 - v0.2.0

- Migrate to `prom-v0.2.8` and `godal-v0.2.5`.
- Add & Update support for Oracle and SQLite.
- Other fixes & enhancements.

## 2020-11-23 - v0.1.0

First release - out-of-the-box universal data access layer implementations for:
- MySQL, PostgreSQL, SQLite
- MongoDB (Standalone & ReplicaSet)
- AWS DynamoDB
