
# Connector MVP Guide: Minimal Viable Connector

This guide provides a streamlined approach to creating a **Minimum Viable Product (MVP)** connector for DataLens. The MVP focuses on core functionality only: connection creation, table listing, and simple query execution with minimal formulas.

**Time Estimate**: 2-3 days for an experienced developer

**What's Included in MVP**:
- Basic connection management
- Table listing
- Simple SELECT queries
- Minimal type support (string, integer, float, boolean, date)
- Essential operators (+, -, *, /, =, !=, <, >, AND, OR)
- Basic aggregations (COUNT, SUM, AVG, MIN, MAX)
- English localization only

**What's NOT in MVP** (add later):
- Advanced formulas (string manipulation, date functions, etc.)
- Complex type handling (arrays, JSON, etc.)
- Error handling and retry logic
- SSL/TLS support
- Multiple authentication methods
- Comprehensive tests
- Localization for other languages

---

## Quick Start Checklist

```
[ ] Step 1: Package structure (30 min)
[ ] Step 2: Core plugin - Constants & DTOs (30 min)
[ ] Step 3: Core plugin - Type transformer (30 min)
[ ] Step 4: Core plugin - Connection & Executor (2 hours)
[ ] Step 5: Core plugin - Data sources (30 min)
[ ] Step 6: Core plugin - Connector assembly (30 min)
[ ] Step 7: API plugin - Schemas (1 hour)
[ ] Step 8: API plugin - Connection form (1 hour)
[ ] Step 9: API plugin - Connector assembly (30 min)
[ ] Step 10: Formula plugin - Minimal definitions (2 hours)
[ ] Step 11: Formula plugin - Connector assembly (30 min)
[ ] Step 12: Localization (30 min)
[ ] Step 13: Basic tests (1 hour)
[ ] Step 14: Integration & verification (1 hour)
```

---

## Step 1: Create Minimal Package Structure

Create this directory structure:

```
mainrepo/lib/dl_connector_mydb/
├── pyproject.toml
├── README.md
├── dl_connector_mydb/
│   ├── __init__.py
│   ├── py.typed
│   ├── core/
│   │   ├── __init__.py
│   │   ├── constants.py
│   │   ├── dto.py
│   │   ├── target_dto.py
│   │   ├── type_transformer.py
│   │   ├── adapters.py
│   │   ├── connection_executors.py
│   │   ├── us_connection.py
│   │   ├── data_source.py
│   │   ├── settings.py
│   │   ├── connector.py
│   │   └── storage_schemas/
│   │       ├── __init__.py
│   │       └── connection.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── connector.py
│   │   ├── connection_info.py
│   │   ├── api_schema/
│   │   │   ├── __init__.py
│   │   │   └── connection.py
│   │   ├── connection_form/
│   │   │   ├── __init__.py
│   │   │   └── form_config.py
│   │   └── i18n/
│   │       ├── __init__.py
│   │       └── localizer.py
│   ├── formula/
│   │   ├── __init__.py
│   │   ├── constants.py
│   │   ├── literal.py
│   │   ├── type_constructor.py
│   │   ├── connector.py
│   │   └── definitions/
│   │       ├── __init__.py
│   │       ├── all.py
│   │       ├── operators_binary.py
│   │       └── functions_aggregation.py
│   ├── db_testing/
│   │   ├── __init__.py
│   │   ├── connector.py
│   │   └── engine_wrapper.py
│   └── locales/
│       └── en/LC_MESSAGES/
│           ├── dl_connector_mydb.po
│           └── dl_connector_mydb.mo
└── dl_connector_mydb_tests/
    ├── __init__.py
    └── unit/
        ├── __init__.py
        └── test_connection.py
```

### Minimal pyproject.toml

```toml
[tool.poetry]
name = "dl-connector-mydb"
version = "0.0.1"
description = "DataLens connector for MyDB (MVP)"
authors = ["Your Name <your.email@example.com>"]
license = "Apache 2.0"
packages = [{include = "dl_connector_mydb"}]

[tool.poetry.dependencies]
python = ">=3.10, <3.13"
attrs = "*"
marshmallow = "*"
sqlalchemy = "*"
dl-core = {path = "../dl_core"}
dl-api-connector = {path = "../dl_api_connector"}
dl-api-commons = {path = "../dl_api_commons"}
dl-constants = {path = "../dl_constants"}
dl-configs = {path = "../dl_configs"}
dl-formula = {path = "../dl_formula"}
dl-i18n = {path = "../dl_i18n"}
dl-type-transformer = {path = "../dl_type_transformer"}
dl-db-testing = {path = "../dl_db_testing"}
dynamic-enum = {path = "../dynamic_enum"}
# Add your database driver here
mydb-driver = "*"

[tool.poetry.group.tests.dependencies]
pytest = "*"
dl-testing = {path = "../dl_testing"}

[tool.poetry.plugins."dl_core.connectors"]
mydb = "dl_connector_mydb.core.connector:MyDBCoreConnector"

[tool.poetry.plugins."dl_api_lib.connectors"]
mydb = "dl_connector_mydb.api.connector:MyDBApiConnector"

[tool.poetry.plugins."dl_formula.connectors"]
mydb = "dl_connector_mydb.formula.connector:MyDBFormulaConnector"

[tool.poetry.plugins."dl_db_testing.connectors"]
mydb = "dl_connector_mydb.db_testing.connector:MyDBDbTestingConnector"

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

---

## Step 2: Core Plugin - Constants & DTOs

### constants.py

```python
from dl_constants.enums import ConnectionType, DataSourceType, SourceBackendType

BACKEND_TYPE_MYDB = SourceBackendType.declare("MYDB")
CONNECTION_TYPE_MYDB = ConnectionType.declare("mydb")
SOURCE_TYPE_MYDB_TABLE = DataSourceType.declare("MYDB_TABLE")
SOURCE_TYPE_MYDB_SUBSELECT = DataSourceType.declare("MYDB_SUBSELECT")
```

### dto.py

```python
from typing import Optional
import attr
from dl_core.connection_models import ConnDTO

@attr.s(frozen=True)
class MyDBConnDTO(ConnDTO):
    conn_id: str = attr.ib()
    host: str = attr.ib()
    port: int = attr.ib()
    username: str = attr.ib()
    password: Optional[str] = attr.ib(repr=False, default=None)
    db_name: Optional[str] = attr.ib(default=None)
```

### target_dto.py

```python
from typing import Optional
import attr
from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO

@attr.s(frozen=True)
class MyDBConnTargetDTO(ConnTargetDTO):
    host: str = attr.ib()
    port: int = attr.ib()
    username: str = attr.ib()
    password: Optional[str] = attr.ib(repr=False, default=None)
    db_name: Optional[str] = attr.ib(default=None)

    def get_sqlalchemy_url(self) -> str:
        auth = f"{self.username}:{self.password}" if self.password else self.username
        url = f"mydb://{auth}@{self.host}:{self.port}"
        if self.db_name:
            url += f"/{self.db_name}"
        return url
```

---

## Step 3: Core Plugin - Type Transformer

### type_transformer.py

```python
from dl_core.db.conversion_base import TypeTransformer, make_native_type
from dl_constants.enums import UserDataType

class MyDBTypeTransformer(TypeTransformer):
    """Minimal type mappings for MVP"""

    native_to_user_map = {
        # Integers
        make_native_type("INTEGER"): UserDataType.integer,
        make_native_type("BIGINT"): UserDataType.integer,
        make_native_type("INT"): UserDataType.integer,

        # Floats
        make_native_type("FLOAT"): UserDataType.float,
        make_native_type("DOUBLE"): UserDataType.float,
        make_native_type("DECIMAL"): UserDataType.float,

        # Strings
        make_native_type("VARCHAR"): UserDataType.string,
        make_native_type("TEXT"): UserDataType.string,
        make_native_type("CHAR"): UserDataType.string,

        # Boolean
        make_native_type("BOOLEAN"): UserDataType.boolean,
        make_native_type("BOOL"): UserDataType.boolean,

        # Date/Time
        make_native_type("DATE"): UserDataType.date,
        make_native_type("TIMESTAMP"): UserDataType.genericdatetime,
    }

    user_to_native_map = {
        UserDataType.integer: make_native_type("BIGINT"),
        UserDataType.float: make_native_type("DOUBLE"),
        UserDataType.string: make_native_type("VARCHAR"),
        UserDataType.boolean: make_native_type("BOOLEAN"),
        UserDataType.date: make_native_type("DATE"),
        UserDataType.datetime: make_native_type("TIMESTAMP"),
        UserDataType.genericdatetime: make_native_type("TIMESTAMP"),
    }
```

---

## Step 4: Core Plugin - Connection & Executor

### adapters.py

```python
from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter

class MyDBDefaultAdapter(BaseClassicAdapter):
    """Use default implementation for MVP"""
    pass
```

### connection_executors.py

```python
from typing import Sequence
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_connector_mydb.core.adapters import MyDBDefaultAdapter
from dl_connector_mydb.core.dto import MyDBConnDTO
from dl_connector_mydb.core.target_dto import MyDBConnTargetDTO

class MyDBConnExecutor(SyncConnExecutorBase):
    TARGET_ADAPTER_CLS = MyDBDefaultAdapter

    def _make_target_conn_dto_pool(self) -> Sequence[MyDBConnTargetDTO]:
        assert isinstance(self._conn_dto, MyDBConnDTO)
        return [
            MyDBConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._pass_db_messages_to_user,
                pass_db_query_to_user=self._pass_db_query_to_user,
                host=self._conn_dto.host,
                port=self._conn_dto.port,
                username=self._conn_dto.username,
                password=self._conn_dto.password,
                db_name=self._conn_dto.db_name,
            )
        ]
```

### settings.py

```python
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_core.connectors_settings import SettingDefinition

class MyDBConnectorSettings(ConnectorSettingsBase):
    ENABLE_TABLE_DATASOURCE_FORM = True

class MyDBSettingDefinition(SettingDefinition):
    settings_class = MyDBConnectorSettings
```

### us_connection.py

```python
from typing import ClassVar
import attr
from dl_core.us_connection_base import (
    ConnectionSQL,
    ConnectionSettingsMixin,
    DataSourceTemplate,
    make_table_datasource_template,
    make_subselect_datasource_template,
)
from dl_i18n.localizer_base import Localizer
from dl_connector_mydb.core.constants import (
    CONNECTION_TYPE_MYDB,
    SOURCE_TYPE_MYDB_TABLE,
    SOURCE_TYPE_MYDB_SUBSELECT,
)
from dl_connector_mydb.core.dto import MyDBConnDTO
from dl_connector_mydb.core.settings import MyDBConnectorSettings

class ConnectionMyDB(
    ConnectionSettingsMixin[MyDBConnectorSettings],
    ConnectionSQL,
):
    conn_type = CONNECTION_TYPE_MYDB
    has_schema: ClassVar[bool] = True
    source_type = SOURCE_TYPE_MYDB_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_MYDB_TABLE, SOURCE_TYPE_MYDB_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    settings_type = MyDBConnectorSettings

    @attr.s(kw_only=True)
    class DataModel(ConnectionSQL.DataModel):
        pass  # Use defaults from parent

    def get_conn_dto(self) -> MyDBConnDTO:
        return MyDBConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            port=self.data.port,
            username=self.data.username,
            password=self.data.password,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return [
            make_table_datasource_template(
                connection_id=self.uuid,
                source_type=SOURCE_TYPE_MYDB_TABLE,
                localizer=localizer,
                schema_name_form_enabled=True,
            ),
            make_subselect_datasource_template(
                connection_id=self.uuid,
                source_type=SOURCE_TYPE_MYDB_SUBSELECT,
                localizer=localizer,
                title="Subselect",
            ),
        ]
```

---

## Step 5: Core Plugin - Data Sources

### data_source.py

```python
from dl_core.data_source.sql import StandardSchemaSQLDataSource, SubselectDataSource
from dl_connector_mydb.core.constants import (
    CONNECTION_TYPE_MYDB,
    SOURCE_TYPE_MYDB_TABLE,
    SOURCE_TYPE_MYDB_SUBSELECT,
)

class MyDBTableDataSource(StandardSchemaSQLDataSource):
    conn_type = CONNECTION_TYPE_MYDB
    source_type = SOURCE_TYPE_MYDB_TABLE

class MyDBSubselectDataSource(SubselectDataSource):
    conn_type = CONNECTION_TYPE_MYDB
    source_type = SOURCE_TYPE_MYDB_SUBSELECT
```

### storage_schemas/connection.py

```python
from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema
from dl_connector_mydb.core.us_connection import ConnectionMyDB

class MyDBConnectionDataStorageSchema(ConnectionSQLDataStorageSchema[ConnectionMyDB.DataModel]):
    TARGET_CLS = ConnectionMyDB.DataModel
```

---

## Step 6: Core Plugin - Connector Assembly

### connector.py

```python
from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)
from dl_core.connectors.sql_base.connector import SQLSubselectCoreSourceDefinitionBase
from dl_core.data_source_spec.sql import StandardSchemaSQLDataSourceSpec
from dl_core.us_manager.storage_schemas.data_source_spec_base import SchemaSQLDataSourceSpecStorageSchema
from dl_connector_mydb.core.adapters import MyDBDefaultAdapter
from dl_connector_mydb.core.connection_executors import MyDBConnExecutor
from dl_connector_mydb.core.constants import *
from dl_connector_mydb.core.data_source import MyDBTableDataSource, MyDBSubselectDataSource
from dl_connector_mydb.core.settings import MyDBSettingDefinition
from dl_connector_mydb.core.storage_schemas.connection import MyDBConnectionDataStorageSchema
from dl_connector_mydb.core.type_transformer import MyDBTypeTransformer
from dl_connector_mydb.core.us_connection import ConnectionMyDB

class MyDBCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_MYDB
    connection_cls = ConnectionMyDB
    us_storage_schema_cls = MyDBConnectionDataStorageSchema
    type_transformer_cls = MyDBTypeTransformer
    sync_conn_executor_cls = MyDBConnExecutor
    dialect_string = "mydb"
    settings_definition = MyDBSettingDefinition

class MyDBCoreTableSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_MYDB_TABLE
    source_cls = MyDBTableDataSource
    source_spec_cls = StandardSchemaSQLDataSourceSpec
    us_storage_schema_cls = SchemaSQLDataSourceSpecStorageSchema

class MyDBCoreSubselectSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_MYDB_SUBSELECT
    source_cls = MyDBSubselectDataSource

class MyDBCoreBackendDefinition(CoreBackendDefinition):
    backend_type = BACKEND_TYPE_MYDB

class MyDBCoreConnector(CoreConnector):
    backend_definition = MyDBCoreBackendDefinition
    connection_definitions = (MyDBCoreConnectionDefinition,)
    source_definitions = (MyDBCoreTableSourceDefinition, MyDBCoreSubselectSourceDefinition)
    rqe_adapter_classes = frozenset({MyDBDefaultAdapter})
```

---

## Step 7: API Plugin - Schemas

### api/api_schema/connection.py

```python
from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_sql import ClassicConnectionSQL
from dl_connector_mydb.core.us_connection import ConnectionMyDB

class MyDBConnectionSchema(ConnectionMetaMixin, ClassicConnectionSQL):
    TARGET_CLS = ConnectionMyDB
```

---

## Step 8: API Plugin - Connection Form

### api/connection_form/form_config.py

```python
from dl_api_connector.form_config.models.base import ConnectionForm, ConnectionFormFactory
from dl_api_connector.form_config.models.api_schema import FormActionApiSchema, FormApiSchema, FormFieldApiSchema
from dl_api_connector.form_config.models.common import CommonFieldName
import dl_api_connector.form_config.models.rows as C
from dl_api_connector.form_config.models.shortcuts.rows import RowConstructor
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_api_commons.base_models import TenantDef
from dl_connector_mydb.api.connection_info import MyDBConnectionInfoProvider

class MyDBConnectionFormFactory(ConnectionFormFactory):
    def get_form_config(
        self,
        connector_settings: ConnectorSettingsBase | None,
        tenant: TenantDef | None,
    ) -> ConnectionForm:
        rc = RowConstructor(localizer=self._localizer)

        return ConnectionForm(
            title=MyDBConnectionInfoProvider.get_title(self._localizer),
            rows=[
                rc.host_row(),
                rc.port_row(default_value="5432"),
                rc.username_row(),
                rc.password_row(mode=self.mode),
                rc.cache_ttl_row(),
            ],
            api_schema=FormApiSchema(
                create=FormActionApiSchema(
                    items=[
                        FormFieldApiSchema(name=CommonFieldName.host, required=True),
                        FormFieldApiSchema(name=CommonFieldName.port, required=True),
                        FormFieldApiSchema(name=CommonFieldName.username, required=True),
                        FormFieldApiSchema(name=CommonFieldName.password, required=True),
                        FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                    ]
                ),
                edit=FormActionApiSchema(
                    items=[
                        FormFieldApiSchema(name=CommonFieldName.host, required=True),
                        FormFieldApiSchema(name=CommonFieldName.port, required=True),
                        FormFieldApiSchema(name=CommonFieldName.username, required=True),
                        FormFieldApiSchema(name=CommonFieldName.password, required=False),
                        FormFieldApiSchema(name=CommonFieldName.cache_ttl_sec, nullable=True),
                    ]
                ),
                check=FormActionApiSchema(
                    items=[
                        FormFieldApiSchema(name=CommonFieldName.host, required=True),
                        FormFieldApiSchema(name=CommonFieldName.port, required=True),
                        FormFieldApiSchema(name=CommonFieldName.username, required=True),
                        FormFieldApiSchema(name=CommonFieldName.password, required=True),
                    ]
                ),
            ),
        )
```

### api/connection_info.py

```python
from dl_api_connector.connection_info import ConnectionInfoProvider
from dl_connector_mydb.api.i18n.localizer import Translatable

class MyDBConnectionInfoProvider(ConnectionInfoProvider):
    title_translatable = Translatable("label_connector-mydb")
```

### api/i18n/localizer.py

```python
from dl_i18n.localizer_base import Localizer

class Translatable:
    """Translation keys"""
    pass

CONFIGS = []  # Minimal for MVP
```

---

## Step 9: API Plugin - Connector Assembly

### api/connector.py

```python
from dl_api_connector.api_schema.source_base import (
    SchematizedSQLDataSourceSchema,
    SchematizedSQLDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiBackendDefinition,
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)
from dl_connector_mydb.api.api_schema.connection import MyDBConnectionSchema
from dl_connector_mydb.api.connection_form.form_config import MyDBConnectionFormFactory
from dl_connector_mydb.api.connection_info import MyDBConnectionInfoProvider
from dl_connector_mydb.core.connector import (
    MyDBCoreBackendDefinition,
    MyDBCoreConnectionDefinition,
    MyDBCoreTableSourceDefinition,
    MyDBCoreSubselectSourceDefinition,
)
from dl_connector_mydb.formula.constants import DIALECT_NAME_MYDB

class MyDBApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = MyDBCoreTableSourceDefinition
    api_schema_cls = SchematizedSQLDataSourceSchema
    template_api_schema_cls = SchematizedSQLDataSourceTemplateSchema

class MyDBApiSubselectSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = MyDBCoreSubselectSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema

class MyDBApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = MyDBCoreConnectionDefinition
    api_generic_schema_cls = MyDBConnectionSchema
    info_provider_cls = MyDBConnectionInfoProvider
    form_factory_cls = MyDBConnectionFormFactory

class MyDBApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = MyDBCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_MYDB

class MyDBApiConnector(ApiConnector):
    backend_definition = MyDBApiBackendDefinition
    connection_definitions = (MyDBApiConnectionDefinition,)
    source_definitions = (MyDBApiTableSourceDefinition, MyDBApiSubselectSourceDefinition)
    translation_configs = frozenset()  # Empty for MVP
```

---

## Step 10: Formula Plugin - Minimal Definitions

### formula/constants.py

```python
from dl_formula.core.dialect import DialectCombo

class MyDBDialect(DialectCombo):
    MYDB = DialectCombo.MYDB

DIALECT_NAME_MYDB = "MYDB"
```

### formula/literal.py

```python
from dl_formula.definitions.literals import Literalizer

class MyDBLiteralizer(Literalizer):
    """Use default implementation for MVP"""
    pass
```

### formula/type_constructor.py

```python
import sqlalchemy as sa
from dl_formula.definitions.type_constructor import TypeConstructor

class MyDBTypeConstructor(TypeConstructor):
    """Minimal type mappings"""
    SQLALCHEMY_TYPES = {
        "integer": sa.Integer,
        "float": sa.Float,
        "string": sa.String,
        "boolean": sa.Boolean,
        "date": sa.Date,
        "datetime": sa.DateTime,
        "genericdatetime": sa.DateTime,
    }
```

### formula/definitions/operators_binary.py

```python
from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common import make_binary_chain
import dl_formula.definitions.operators_binary as base

# Essential operators for MVP
DEFINITIONS_BINARY = [
    # Arithmetic
    base.BinaryPlus.for_dialect(MyDBDialect.MYDB),
    base.BinaryMinus.for_dialect(MyDBDialect.MYDB),
    base.BinaryMultiply.for_dialect(MyDBDialect.MYDB),
    base.BinaryDivide.for_dialect(MyDBDialect.MYDB),

    # Comparison
    base.BinaryEqualTo.for_dialect(MyDBDialect.MYDB),
    base.BinaryNotEqualTo.for_dialect(MyDBDialect.MYDB),
    base.BinaryLessThan.for_dialect(MyDBDialect.MYDB),
    base.BinaryLessThanOrEqual.for_dialect(MyDBDialect.MYDB),
    base.BinaryGreaterThan.for_dialect(MyDBDialect.MYDB),
    base.BinaryGreaterThanOrEqual.for_dialect(MyDBDialect.MYDB),

    # Logical
    base.BinaryAnd.for_dialect(MyDBDialect.MYDB),
    base.BinaryOr.for_dialect(MyDBDialect.MYDB),
]
```

### formula/definitions/functions_aggregation.py

```python
import dl_formula.definitions.functions_aggregation as base

# Essential aggregations for MVP
DEFINITIONS_AGG = [
    base.FuncCount.for_dialect(MyDBDialect.MYDB),
    base.FuncSum.for_dialect(MyDBDialect.MYDB),
    base.FuncAvg.for_dialect(MyDBDialect.MYDB),
    base.FuncMin.for_dialect(MyDBDialect.MYDB),
    base.FuncMax.for_dialect(MyDBDialect.MYDB),
]
```

### formula/definitions/all.py

```python
from dl_connector_mydb.formula.definitions.operators_binary import DEFINITIONS_BINARY
from dl_connector_mydb.formula.definitions.functions_aggregation import DEFINITIONS_AGG

DEFINITIONS = [
    *DEFINITIONS_BINARY,
    *DEFINITIONS_AGG,
]
```

---

## Step 11: Formula Plugin - Connector Assembly

### formula/connector.py

```python
from dl_formula.connectors.base.connector import FormulaConnector
from dl_connector_mydb.formula.constants import MyDBDialect, DIALECT_NAME_MYDB
from dl_connector_mydb.formula.definitions.all import DEFINITIONS
from dl_connector_mydb.formula.literal import MyDBLiteralizer
from dl_connector_mydb.formula.type_constructor import MyDBTypeConstructor

# Import your SQLAlchemy dialect
from mydb.sqlalchemy import MyDBDialect as SAMyDBDialect

class MyDBFormulaConnector(FormulaConnector):
    dialect_ns_cls = MyDBDialect
    dialects = MyDBDialect.MYDB
    default_dialect = MyDBDialect.MYDB
    op_definitions = DEFINITIONS
    literalizer_cls = MyDBLiteralizer
    type_constructor_cls = MyDBTypeConstructor
    sa_dialect = SAMyDBDialect()
```

---

## Step 12: Localization (Minimal)

### locales/en/LC_MESSAGES/dl_connector_mydb.po

```po
msgid ""
msgstr ""
"Content-Type: text/plain; charset=UTF-8\n"

msgid "label_connector-mydb"
msgstr "MyDB"
```

Compile with: `msgfmt dl_connector_mydb.po -o dl_connector_mydb.mo`

---

## Step 13: DB Testing Connector

### db_testing/connector.py

```python
from dl_db_testing.connectors.base.connector import DbTestingConnector

class MyDBDbTestingConnector(DbTestingConnector):
    pass  # Minimal implementation
```

### db_testing/engine_wrapper.py

```python
from dl_db_testing.database.engine_wrapper import EngineWrapperBase

class MyDBEngineWrapper(EngineWrapperBase):
    URL_PREFIX = "mydb"
```

---

## Step 14: Basic Smoke Test

### dl_connector_mydb_tests/unit/test_connection.py

```python
import pytest
from dl_connector_mydb.core.us_connection import ConnectionMyDB
from dl_connector_mydb.core.constants import CONNECTION_TYPE_MYDB

def test_connection_type():
    """Verify connection type is registered"""
    assert CONNECTION_TYPE_MYDB.value == "mydb"

def test_connection_creation():
    """Verify connection can be instantiated"""
    conn = ConnectionMyDB.create_from_dict(
        {
            "host": "localhost",
            "port": 5432,
            "username": "test",
            "password": "test",
        },
        ds_key="test_conn",
        type_=CONNECTION_TYPE_MYDB.value,
    )
    assert conn.conn_type == CONNECTION_TYPE_MYDB
    assert conn.data.host == "localhost"
```

---

## Step 15: Integration & Verification

### Add to Application

Edit `mainrepo/app/dl_data_api/pyproject.toml`:

```toml
[tool.poetry.dependencies]
dl-connector-mydb = {path = "../../lib/dl_connector_mydb"}

[tool.deptry.per_rule_ignores]
DEP002 = [
  "dl-connector-mydb",
]
```

### Verification Steps

1. **Install the connector**:
   ```bash
   cd mainrepo/lib/dl_connector_mydb
   poetry install
   ```

2. **Run unit tests**:
   ```bash
   poetry run pytest dl_connector_mydb_tests/unit/
   ```

3. **Build the application**:
   cd mainrepo/app/dl_data_api
   poetry install
   ```

4. **Start the application** and verify the connector appears in the UI

5. **Test end-to-end**:
   - Create a connection using the UI
   - List tables from your database
   - Create a dataset
   - Execute a simple query with basic formulas

---

## Common Issues and Solutions

### Issue: Plugin not loading

**Symptom**: Connector doesn't appear in UI

**Solutions**:
- Verify plugin registration in `pyproject.toml`
- Check that the connector is in application dependencies
- Restart the application after adding the connector
- Check logs for import errors

### Issue: Connection fails

**Symptom**: Cannot connect to database

**Solutions**:
- Verify SQLAlchemy URL format in [`target_dto.py`](../../lib/dl_connector_trino/dl_connector_trino/core/target_dto.py)
- Check database driver is installed
- Verify connection parameters (host, port, credentials)
- Test connection directly with SQLAlchemy

### Issue: Type conversion errors

**Symptom**: Data types not recognized

**Solutions**:
- Add missing type mappings in [`type_transformer.py`](../../lib/dl_connector_trino/dl_connector_trino/core/type_transformer.py)
- Check database's native type names
- Verify both directions of mapping (native→user and user→native)

### Issue: Formula compilation errors

**Symptom**: Formulas don't work or produce SQL errors

**Solutions**:
- Verify formula definitions import the correct base classes
- Check that `MyDBDialect` is used consistently
- Test SQL output manually against your database
- Start with minimal operators and add more incrementally

---

## Next Steps After MVP

Once your MVP is working, consider adding:

1. **More Formula Functions**:
   - String functions (CONCAT, SUBSTRING, UPPER, LOWER)
   - Date functions (DATE_PART, DATE_TRUNC, NOW)
   - Type conversion functions (CAST, STR, INT, FLOAT)
   - Window functions (ROW_NUMBER, RANK, LAG, LEAD)

2. **Enhanced Connection Features**:
   - SSL/TLS support
   - Multiple authentication methods
   - Connection pooling
   - Timeout configuration

3. **Better Error Handling**:
   - Custom error transformer
   - Retry logic
   - User-friendly error messages

4. **More Data Types**:
   - Arrays
   - JSON/JSONB
   - UUID
   - Geometric types
   - Custom types

5. **Comprehensive Testing**:
   - Integration tests with real database
   - Formula tests for all functions
   - Connection form tests
   - Performance tests

6. **Additional Features**:
   - Query optimization
   - Custom query compiler
   - Data source migration support
   - Advanced connection options

7. **Documentation**:
   - User guide
   - API documentation
   - Examples and tutorials
   - Troubleshooting guide

8. **Localization**:
   - Russian translations
   - Other languages as needed

---

## MVP Completion Checklist

Before considering your MVP complete, verify:

- [ ] **Package Structure**
  - [ ] All required directories created
  - [ ] `pyproject.toml` configured correctly
  - [ ] All `__init__.py` files present
  - [ ] `py.typed` file present

- [ ] **Core Plugin**
  - [ ] Constants defined
  - [ ] DTOs implemented
  - [ ] Type transformer with basic types
  - [ ] Connection executor works
  - [ ] Connection class complete
  - [ ] Data sources defined
  - [ ] Storage schemas created
  - [ ] Connector assembled

- [ ] **API Plugin**
  - [ ] Connection schema defined
  - [ ] Connection form renders
  - [ ] Connection info provider set up
  - [ ] Connector assembled

- [ ] **Formula Plugin**
  - [ ] Dialect constants defined
  - [ ] Basic operators work
  - [ ] Basic aggregations work
  - [ ] Connector assembled

- [ ] **Testing**
  - [ ] DB testing connector created
  - [ ] Basic unit test passes
  - [ ] Can create connection programmatically

- [ ] **Integration**
  - [ ] Added to application dependencies
  - [ ] Application builds successfully
  - [ ] Connector appears in UI

- [ ] **End-to-End Verification**
  - [ ] Can create connection via UI
  - [ ] Can list tables
  - [ ] Can create dataset
  - [ ] Can execute simple query
  - [ ] Basic formulas work (SUM, COUNT, etc.)

---

## Time Breakdown

Based on the checklist, here's a realistic time breakdown:

| Step | Task | Time |
|------|------|------|
| 1 | Package structure | 30 min |
| 2 | Core: Constants & DTOs | 30 min |
| 3 | Core: Type transformer | 30 min |
| 4 | Core: Connection & Executor | 2 hours |
| 5 | Core: Data sources | 30 min |
| 6 | Core: Connector assembly | 30 min |
| 7 | API: Schemas | 1 hour |
| 8 | API: Connection form | 1 hour |
| 9 | API: Connector assembly | 30 min |
| 10 | Formula: Minimal definitions | 2 hours |
| 11 | Formula: Connector assembly | 30 min |
| 12 | Localization | 30 min |
| 13 | Basic tests | 1 hour |
| 14 | Integration & verification | 1 hour |
| **Total** | | **~12 hours** |

Add buffer time for:
- Learning the codebase (if new): +4-8 hours
- Debugging issues: +2-4 hours
- Database-specific quirks: +2-4 hours

**Total realistic estimate: 2-3 days**

---

## Summary

This MVP guide provides a streamlined path to creating a functional DataLens connector with:

- **Minimal complexity**: Only essential features
- **Fast iteration**: Get something working quickly
- **Clear structure**: Step-by-step with complete code
- **Practical focus**: Connection, tables, basic queries

Once your MVP is working, you can incrementally add features from the [complete connector creation plan](connector_creation_plan.md).

**Key Success Factors**:
- Follow the steps in order
- Test each component as you build it
- Use existing connectors as references
- Start simple, add complexity later
- Focus on getting end-to-end flow working first

**Reference Files**:
- Use [Trino connector](../../lib/dl_connector_trino) as primary reference
- Check [PostgreSQL connector](../../lib/dl_connector_postgresql) for simpler patterns
- Refer to [connector development guide](connector_development.md) for concepts

Good luck with your connector development!
