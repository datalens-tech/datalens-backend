# Connector development

## Before you begin

This guide covers the basics of connector plugins, it is intended to be a reference point before starting the development,
it sets up the terminology, but it cannot cover every aspect of development, therefore further digging into the code
to gather examples is most certainly required to implement a new connector.

After getting familiar with the plugin system and key entities of each plugin, one may refer to existing connectors
to see concrete implementations and use them as a foundation of a new connector. These connectors are worth noting:
- [BigQuery](https://github.com/datalens-tech/datalens-backend/tree/c56fc958ff21bd5a2d1733fb97af7bd8dfad0691/lib/dl_connector_bigquery) and [Snowflake](https://github.com/datalens-tech/datalens-backend/tree/c56fc958ff21bd5a2d1733fb97af7bd8dfad0691/lib/dl_connector_snowflake) connectors – these connectors were added relatively recently, they have their plugins and implementations in a cleaner way
- [ClickHouse](https://github.com/datalens-tech/datalens-backend/tree/c56fc958ff21bd5a2d1733fb97af7bd8dfad0691/lib/dl_connector_clickhouse/dl_connector_clickhouse) connector – one the most elaborate connectors

This guide does not yet cover:
- any tricks that are required to create non-sql, file or API connectors; terms "source", "data source" and "database" in this guide may be used interchangeably
- localization
- auto-generated documentation (dl-formula-ref plugins)
- in-depth overview of each class capabilities or the interaction of connections and datasets

## Connector basics and plugin system

Each connector is represented by a set of plugins.

Plugins are implemented as entrypoints that point to the connector class of a particular plugin.

Connector classes are registered in the system [automatically](https://github.com/datalens-tech/datalens-backend/blob/c56fc958ff21bd5a2d1733fb97af7bd8dfad0691/app/dl_data_api/dl_data_api/app.py#L50 )
at the start of the application, therefore, to include a specific connector in the build, it should be listed among
application package dependencies ([example](https://github.com/datalens-tech/datalens-backend/blob/c56fc958ff21bd5a2d1733fb97af7bd8dfad0691/app/dl_data_api/pyproject.toml#L21)).

In general, the implementation of three plugins is necessary for the connector to function:
- dl-core plugin – everything related to connecting to the source, query execution, and storing the connection in
  the US – the base part of the plugin is located in the `dl_core` package

- dl-api plugin – defines (de-)serialization schemas for the API, specifies the core connector to use and defines
  the connection form schema – the base part of the plugin is located in the `dl_api_connector` package, a significant 
  part of the plugin, including registration, is in the `dl_api_lib` package

- dl-formula plugin – defines the rules for translating formulas from the internal DL language into SQL for a specific
  source and sqlalchemy dialect – the basic part of the plugin is located in the `dl_formula` package

- (optional) dl-formula-ref plugin – defines a dialect for rendering auto–generated formula documentation,
  allows one to add connector-specific refinements to existing formulas or add new ones – the basic part of the plugin
  is located in the `dl_formual_ref` package

Connection objects, as well as datasets, are stored in the [United Storage](https://github.com/datalens-tech/datalens-us), so each object, among other things, defines a storage schema in the form of a marshmallow schema.

[SQLAlchemy](https://www.sqlalchemy.org/) is used for query preparation, so a corresponding sqlalchemy dialect must be present in the dependencies of the connector package.

Each plugin defines a number of constants that are used for its registration, they are usually placed in the `constants` module within the plugin.

For a more detailed overview of each plugin, see:
- [About dl-core plugin](plugins/dl_core_plugin.md)
- [About dl-api plugin](plugins/dl_api_plugin.md)
- [About dl-formula plugin](plugins/dl_formula_plugin.md)
