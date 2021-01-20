# henge

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/henge)](https://goreportcard.com/report/github.com/btnguyen2k/henge)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/henge)](https://pkg.go.dev/github.com/btnguyen2k/henge)
[![Actions Status](https://github.com/btnguyen2k/henge/workflows/Henge/badge.svg)](https://github.com/btnguyen2k/henge/actions)
[![codecov](https://codecov.io/gh/btnguyen2k/henge/branch/main/graph/badge.svg?token=EF3O2PB9PQ)](https://codecov.io/gh/btnguyen2k/henge)
[![Release](https://img.shields.io/github/release/btnguyen2k/henge.svg?style=flat-square)](RELEASE-NOTES.md)

An out-of-the-box NoSQL-style universal data access layer implementation for Go.

## Documentation

`henge` aims to free application and developer from the headache of implementing low-level underlying data access layer (DAL) by
offering a universal DAL implementation.

At a glance, `henge` can fit into application's data access layers as illustrated in the following diagram:

![henge-design-full](./docs/henge-design-full.png)

**Data storage layer**

`henge` provides a ready-to-use data storage layer that supports (since [v0.3.0](RELEASE-NOTES.md)):
- Azure Cosmos DB
- AWS DynamoDB
- MongoDB
- MySQL
- MSSQL
- PostgreSQL
- Oracle
- SQLite (for non-production only, i.e. testing, poc, demo)

`henge` uses the following schema for data storage layer (*):
- Field/column `zid`: business object's unique id, implemented as primary key (for SQL storage) or partition key (for no-SQL storage).
  Its data type is VARCHAR or string (whichever is best fit with the underlying data store).
- Field/column `zdata`: store business object's member values. Its data type is TEXT, CLOB, JSONB or string (whichever is best fit with the underlying data store).
- Field/column `zchecksum`: checksum of business object data. Its data type is VARCHAR or string (whichever is best fit with the underlying data store).
- Field/column `ztcreated` and `ztupdated`: timestamp when business object is created/last updated. Its data type is DATETIME, TIMESTAMP or string (whichever is best fit with the underlying data store).
- Field/column `ztversion`: for application's internal use (can be used for compatibility check or data migration). Its data type is BIGINT, INT or number (whichever is best fit with the underlying data store).

_(*) column names and data types may differ depends on the underlying data store._

`henge` also allows application to define its own fields/columns.

**"Universal" Business Object (BO) and Data Access Object (DAO) implementations**

`henge` also provides ready-to-use BO & DAO implementation to interact with the data storage layer.
- `UniversalBo`: no-SQL style universal business object. `UniversalBo` provide APIs to store/retrieve business attributes in hierarchy manner.
- `UniversalDao`: API interface to store/retrieve `UniversalBo` to/from data store. `henge` provides the following implementations:
  - `UniversalDaoDynamodb`: AWS DynamoDB-specific implementation, use [github.com/aws/aws-sdk-go](https://github.com/aws/aws-sdk-go) to access AWS DynamoDB.
  - `UniversalDaoMongo`: MongoDB-specific implementation, use [go.mongodb.org/mongo-driver/mongo](https://go.mongodb.org/mongo-driver/mongo) to access MongoDB server/cluster.
  - `UniversalDaoSql`: SQL-specific implementation. Since [v0.2.0](RELEASE-NOTES.md), `henge` supports the following database systems:
    - MSSQL: use driver [github.com/denisenkom/go-mssqldb](https://github.com/denisenkom/go-mssqldb)
    - MySQL: use driver [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)
    - Oracle: use driver [github.com/godror/godror](https://github.com/godror/godror)
    - PostgreSQL: use driver [github.com/jackc/pgx](https://github.com/jackc/pgx)
    - SQLite: use driver [github.com/mattn/go-sqlite3](https://github.com/mattn/go-sqlite3)
  - `UniversalDaoCosmosdb`: (since [v0.3.0](RELEASE-NOTES.md)) Azure Cosmos DB-specific implementation, built on top `UniversalDaoSql`, use driver [github.com/btnguyen2k/gocossmos](https://github.com/btnguyen2k/gocossmos).

**(Optional) Custom Business Object (BO) and Data Access Object (DAO) implementations**

Application can have its own BO and DAO implemenations. See source code of test scripts for examples.

## License

MIT - see [LICENSE.md](LICENSE.md).
