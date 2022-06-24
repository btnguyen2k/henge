# henge release notes

## 2022-06-24 - v0.5.7

Allow time-layout and timestamp-rouding can be setup once when creating DAO instances.
- `NewUniversalDaoMongo`: accept new optional argument `defaultUboOpts ...UboOpt`.
- `NewUniversalDaoDynamodb`: accept new optional argument `defaultUboOpts ...UboOpt`.
- `NewUniversalDaoSql`: accept new optional argument `defaultUboOpts ...UboOpt`.

## 2022-02-07 - v0.5.6

- Enhance `UniversalBo` to better support values of type `time.Time`:
  - New function `UniversalBo.RoundTimestamp(t time.Time) time.Time`.
  - New function `UniversalBo.NormalizeTimestampForStoring(t time.Time, layout string) string`.

## 2022-01-09 - v0.5.5

- Rework `UniversalBo` to better handle value of type `time.Time` (and `*time.Time`):
  - `UniversalBo.SetDataAttr` will convert value of type `time.Time` (and `*time.Time`) to string using layout `DefaultTimeLayout` before storing.
  - time-layout (and timestamp-rounding) should be private to `UniversalBo` (and `henge`).

## 2021-12-16 - v0.5.4

- (Breaking change) Migrate timestamp-rounding and time-layout settings to `UniversalBo`. These settings are now per-`UniversalBo` instance, no longer at `henge`'s package-level.
- Introduce default timestamp-rounding and time-layout settings at `henge`'s package level.

## 2021-12-12 - v0.5.3

- New type `UboSyncOpts`.
- `UniversalBo.Sync()` and `UniversalBo._sync()` now accept optional array of `UboSyncOpts` as parameter.
- `henge` call `UniversalBo.Sync()` and `UniversalBo._sync()` with `UboSyncOpts{UpdateTimestampIfChecksumChange: true}`
  by default.

## 2021-11-03 - v0.5.2

- Migrated to `prom v0.2.15`, `UniversalDaoDynamodb` now supports basic sorting (via GSI).
- Update dependencies.
- Other fixes and enhancements.

## 2021-05-17 - v0.5.1

- Fix `UniversalBo.Clone()`: call `_parseDataJson` before returning.

## 2021-04-30 - v0.5.0

- Migrate to `btnguyen2k/godal v0.5.0`: implement `godal.SortingOpt` and `godal.FilterOpt`.

## 2021-04-05 - v0.4.1

- Fix checksum value mismatched when storing to/retrieving from storage.
- New public variable `hange.TimeLayout` to control datetime format (default value is `time.RFC3339`).
- New public variable `henge.TimestampRounding` to control how `UniversalBo` would round timestamp before storing.
- Other fixes and enhancements.

## 2021-03-24 - v0.4.0

- Bump `btnguyen2k/godal` to `v0.4.0`.

## 2021-02-06 - v0.3.2

- `UniversalDaoDynamodb`: add `pkPrefix` and `pkPrefixValue`, supporting multi-tenant DynamoDB tables.
- `UniversalDaoCosmosdbSql`: support multi-tenant CosmosDB collections.
- **Breaking changes**:
    - New structs `DynamodbTablesSpec` and `DynamodbDaoSpec`, signatures of functions `InitDynamodbTables`
      and `NewUniversalDaoDynamodb` changed.
    - New structs `CosmosdbCollectionSpec` and `CosmosdbDaoSpec`, signatures of functions `InitCosmosdbCollection`
      and `NewUniversalDaoCosmosdbSql` changed.

## 2021-01-20 - v0.3.1

- Function `InitDynamodbTable` is deprecated, use `InitDynamodbTables` instead.
- Add support for [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/introduction)
  using [btnguyen2k/gocosmos](https://github.com/btnguyen2k/gocosmos) driver.
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
