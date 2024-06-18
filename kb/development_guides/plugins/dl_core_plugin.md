## About dl-core plugin

Each connector implements a number of interfaces by inheriting from the base classes defined in the `dl_core` package.

To connect the plugin, the connector defines "definition classes" (`CoreSourceDefinition`, `CoreConnectionDefinition`, `CoreBackendDefinition`),
which specify implementations of all connector-specific interfaces - the base definition classes are contained in the module `dl_core.connectors.base.connector`.

The created definition classes, in turn, are assembled into a "connector class", which is inherited from the `CoreConnector` class.

The created connector class is registered as an entrypoint.

[Example](https://github.com/datalens-tech/datalens-backend/blob/302a63cfed443ce4520a7bbd959a3d2e6b298189/lib/dl_connector_clickhouse/pyproject.toml#L45-L46) of an entrypoint registration:
```toml
[tool.poetry.plugins."dl_core.connectors"]
clickhouse = "dl_connector_clickhouse.core.clickhouse.connector:ClickHouseCoreConnector"
```

Thus, the core-plugin consists of 4 parts:
- `CoreBackendDefinition` – query compilation features, backend type discriminator (backend_type)

- `CoreConnectionDefinition` – the connection object, 
  source type conversion rules, 
  connection & data receiving mechanisms, 
  a (de-)serialization schema for storing in the US (commonly referred to as a storage schema),
  connector-specific settings
  – there may be more than one definition for a single connector

- `CoreSourceDefinition` – the DataSource object, its specification and storage schema – there is usually more than one definition for a single connector

- `CoreConnector` – combines the definition classes listed above and defines the mapping of sqlalchemy type generators to the data types of the source

The main interfaces implemented by the connector for each of the parts are described below.


### CoreSourceDefinition

<details>
<summary>Expand</summary>

DataSource is a part of a dataset, but it is tightly bound to the connection it belongs to.

When creating a dataset, the list of DataSources is obtained from the connection as a list of parameter sets necessary for initializing the DataSource.

After a specific set of parameters is added to a dataset, a DataSource is built based on it, which is added to the dataset.

#### DataSource and DataSourceSpec

DataSourceSpec contains the coordinates of the data source inside the user source (for example, with a database as a user source,
a combination of a schema name and a table name make up a DataSourceSpec).

DataSource is a wrapper for accessing DataSourceSpec data.

In most cases, `SqlDataSource` or `StandardSQLDataSource` will be suitable base classes.

A number of convenient base classes are implemented for commonly used source types (for example, `SubselectDataSource` for dataset subqueries)

The main tasks of DataSource/DataSourceSpec:
- storing the raw_schema, which is a representation of the data schema of a specific source (table) in the database
- providing sql-source (the FROM section of a query)
- providing source parameters (DataSourceSpec data for source creation and replacement in a dataset)
- checking the existence of a DataSource in the database

#### DataSourceSpec storage schema

DataSources are stored only as part of a dataset, according to this marshmallow schema.

</details>


### CoreBackendDefinition

<details>
<summary>Expand</summary>

Allows specification of a custom sqlalchemy `Query` class.

Allows specification of a custom query compiler, which in turn controls quotation, column aliasing, group by policy, etc.

</details>


### CoreConnectionDefinition

<details>
<summary>Expand</summary>

#### TypeTransformer

The `TypeTransformer` is responsible for casting values from the source's native format into DataLens & python inner type systems and vice versa.

#### ConnExecutor

Connection executor combines sync and async adapters, keeps their initialization in one place and provides acts as a
wrapper for them (to check table existence, list tables, execute queries, etc.).
Sync and async connection executors are usually used in their respective environments (sync or async).

Usually it is enough to inherit your ConnExecutor from `DefaultSqlAlchemyConnExecutor` and extend it with:
- an implementation of `_make_target_conn_dto_pool()`, which is a collection of `ConnTargetDTO`s (usually the only difference between its items is the host)
- a reference to a target adapter class, that will be used by the executor

#### Connection

A representation of a DataLens connection entity.

Uses ConnExecutor to access the source for a list of tables, verify the existence of a table, etc.

Defines a `DataModel` – this is the connection data itself, some of which is stored in the US (according to its US storage schema).

Performs data validation when the connection is changed (`validate_new_data`).

Предоставляет `ConnDTO` (`get_conn_dto()`), который содержит данные для подключения к источнику и используется для построения `ConnExecutor`а,
также может использоваться для получения данных подключения при, например, логировании.
Provides a `ConnDTO` (`get_conn_dto()`), which contains source connection parameters and is used to build a `ConnExecutor`.
Note: `ConnDTO` is translated into a `TargetConnDTO` by the connection executor, it can additionally be divided into multiple DTOs (one per host) and can be supplemented with connection options.

See `ClassicConnectionSQL` and its inheritors for examples.

</details>


### CoreConnector

<details>
<summary>Expand</summary>

Combines all the definitions described above, may add optional configurations to them, such as notification classes or security settings.

</details>
