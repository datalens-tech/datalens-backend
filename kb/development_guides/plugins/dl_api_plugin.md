## About dl-api plugin

Each connector implements a number of interfaces by inheriting from the base classes defined in the `dl_api_connector` package.

To implement the plugin, the connector defines "definition classes" (`ApiSourceDefinition`, `ApiConnectionDefinition`, `ApiBackendDefinition`),
which specify implementations of all connector-specific interfaces - the base definition classes are contained in the module `dl_api_connector.connector`.

The created definition classes, in turn, are assembled into a "connector class" inherited from the `ApiConnector` class.

The created connector class is registered as an entrypoint.

[Example](https://github.com/datalens-tech/datalens-backend/blob/47345c2d7c8f008c03e77054211c0f193ca1c929/lib/dl_connector_clickhouse/pyproject.toml#L42-L43) of an entrypoint registration:
```toml
[tool.poetry.plugins."dl_api_lib.connectors"]
clickhouse = "dl_connector_clickhouse.api.connector:ClickHouseApiConnector"
```

Thus, the api-plugin consists of 4 parts:
- `ApiBackendDefinition` – refers to the `CoreBackendDefinition` to use, sets the dialect name and optional query generation settings

- `ApiConnectionDefinition` – refers to the `CoreConnectionDefinition` to use, sets the `ConnectionInfoProvider`, API schema and a connection form factory

- `ApiSourceDefinition` – refers to the `CoreSourceDefinition` to use, sets its API schema and the source template schema

- `ApiConnector` – combines the definition classes listed above and defines localization configs

The main interfaces implemented by the connector for each of the parts are described below.


### ApiSourceDefinition

<details>
<summary>Expand</summary>

The Template API schema is defined in the form in which:
- the DataSource is returned from the connection when the DataSource list is requested (`/<connection_id>/info/sources` handler)
- the DataSource is accepted by the dataset when it is added

The source is added to the dataset as follows:
- a list of DataSource templates (sets of parameters) is obtained from the connection via the `/<connection_id>/info/sources` handler
- ordinary DataSources are sent to the dataset to be added via the `add_source` action as is 
- `freeform_sources` are sent in the same way, but with additional input from the user

The API schema is defined in the form in which the DataSource is accepted when its schema is requested (`/<connection_id>/info/source/schema` handler).

</details>


### ApiBackendDefinition

<details>
<summary>Expand</summary>

Additional settings control the way the query is built, base specification is enough for most of the connectors. See `ApiBackendDefinition` inheritors for examples.

</details>


### ApiConnectionDefinition

<details>
<summary>Expand</summary>

#### ConnectionInfoProvider

ConnectionInfoProvider is used to get the localized name of the connection type and the alias by which the creation form can be accessed
in addition to its conn_type version, that is, if an alias is specified, both links will work:
- `https://<datalens-host>/connections/new/<conn_type>`
- `https://<datalens-host>/connections/new/<conn_alias>`

#### ConnectionFormFactory

Form factory is used to build backend driven forms when creating and editing connections, the form config consists of two main parts:
- the layout of the inputs
- the schema for sending their values to the API

#### ConnectionSchema

Marshmallow schema that contains fields for both issuing and creating/editing a connection object.
The `dl_api_connector.api_schema` contains a number of mixins to identify commonly used parts of a schema (raw_sql_level, cache TTL, etc.).

</details>


### ApiConnector

<details>
<summary>Expand</summary>

Combines all the definitions described above.

#### Translation configs

A set of `TranslationConfig` objects that specify translations of connector-specific texts, such as the connection type name and texts for its form labels.

</details>
