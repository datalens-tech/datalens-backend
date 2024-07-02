## About dl-formula plugin

DL Formula plugin consists of a single connector class, `FormulaConnector`.

It defines the dialect specification in terms of DataLens (`DialectCombo`, `DialectNamespace`) and in terms of sqlalchemy (a class implementing an SA dialect).

The created connector class is registered as an entrypoint.

[Example](https://github.com/datalens-tech/datalens-backend/blob/c56fc958ff21bd5a2d1733fb97af7bd8dfad0691/lib/dl_connector_clickhouse/pyproject.toml#L51-L52) of an entrypoint registration:
```toml
[tool.poetry.plugins."dl_formula.connectors"]
clickhouse = "dl_connector_clickhouse.formula.connector:ClickHouseFormulaConnector"
```

### Formula definitions

A list of DL formulas supported in the dialect, the added formulas can either have an explicit implementation,
or copy the basic implementation from `dl_formula` for the new dialect.

See [ClickHouse string function definitions](https://github.com/datalens-tech/datalens-backend/blob/c56fc958ff21bd5a2d1733fb97af7bd8dfad0691/lib/dl_connector_clickhouse/dl_connector_clickhouse/formula/definitions/functions_string.py) for examples.

### Literalizer

Presents a fine-tuned version of `sqlalchemy.literal`.

### TypeConstructor

Defines how DL types are mapped to sqlalchemy types of a specific dialect; used only in tests and for documentation generation.
