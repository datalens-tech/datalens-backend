from enum import Enum
from typing import NamedTuple

from bi_formula.core.datatype import DataType
from bi_formula.core.dialect import StandardDialect as D, DialectCombo

from bi_connector_clickhouse.formula.constants import ClickHouseDialect
from bi_connector_mysql.formula.constants import MySQLDialect
from bi_connector_yql.formula.constants import YqlDialect
from bi_connector_metrica.formula.constants import MetricaDialect
from bi_connector_oracle.formula.constants import OracleDialect
from bi_connector_mssql.formula.constants import MssqlDialect
from bi_connector_postgresql.formula.constants import PostgreSQLDialect

from bi_formula_ref.localization import get_gettext


_ = get_gettext()

EXAMPLE_TITLE = _('Example')
DOC_TOC_TITLE = _('Function Reference')
DOC_ALL_TITLE = _('All Functions')
DOC_OVERVIEW_TEXT = _('Overview')
DOC_AVAIL_TITLE = _('Function Availability')
COMMON_TYPE_NOTE = _('Arguments ({args}) must be of the same type.')
CONST_TYPE_NOTE = _('Only constant values are accepted for the arguments ({args}).')
ALSO_IN_OTHER_CATEGORIES = _(
    'Function `{func_name}` is also found in the following categories: '
)
DEPENDS_ON_ARGS = _('Depends on argument types')
FROM_ARGS = _('Same type as ({args})')
ANY_TYPE = _('Any')
HUMAN_CATEGORIES = {
    'aggregation': _('Aggregate functions'),
    'string': _('String functions'),
    'numeric': _('Mathematical functions'),
    'logical': _('Logical functions'),
    'date': _('Date/Time functions'),
    'type-conversion': _('Type conversion functions'),
    'operator': _('Operators'),
    'geographical': _('Geographical functions'),
    'markup': _('Text markup functions'),
    'window': _('Window functions'),
    'time-series': _('Time series functions'),
    'array': _('Array functions'),
}
FUNCTION_CATEGORY_TAG = {
    'aggregation': _('aggregation'),
    'string': _('string'),
    'numeric': _('mathematical'),
    'logical': _('logical'),
    'date': _('date/time'),
    'type-conversion': _('type conversion'),
    'operator': _('operator'),
    'geographical': _('geographical'),
    'markup': _('text markup'),
    'window': _('window'),
    'time-series': _('time series'),
    'array': _('array'),
}
HUMAN_DATA_TYPES = {
    DataType.INTEGER: _('Integer'),
    DataType.FLOAT: _('Fractional number'),
    DataType.BOOLEAN: _('Boolean'),
    DataType.STRING: _('String'),
    DataType.DATE: _('Date'),
    DataType.DATETIME: _('Datetime (deprecated)'),
    DataType.DATETIMETZ: _('Datetime with timezone'),
    DataType.GENERICDATETIME: _('Datetime'),
    DataType.GEOPOINT: _('Geopoint'),
    DataType.GEOPOLYGON: _('Geopolygon'),
    DataType.MARKUP: _('Markup'),
    DataType.UUID: _('UUID'),
    DataType.ARRAY_FLOAT: _('Array of fractional numbers'),
    DataType.ARRAY_INT: _('Array of integers'),
    DataType.ARRAY_STR: _('Array of strings'),
    DataType.TREE_STR: _('Tree'),
    (
        DataType.FLOAT,
        DataType.INTEGER,
        DataType.BOOLEAN,
        DataType.STRING,
        DataType.DATE,
        DataType.DATETIME,
        DataType.GENERICDATETIME,
        DataType.GEOPOINT,
        DataType.GEOPOLYGON,
        DataType.MARKUP,
        DataType.UUID,
        DataType.ARRAY_FLOAT,
        DataType.ARRAY_INT,
        DataType.ARRAY_STR,
    ): ANY_TYPE,  # TODO: remove after DATETIMETZ is fully adopted
    (
        DataType.FLOAT,
        DataType.INTEGER,
        DataType.BOOLEAN,
        DataType.STRING,
        DataType.DATE,
        DataType.DATETIME,
        DataType.DATETIMETZ,
        DataType.GENERICDATETIME,
        DataType.GEOPOINT,
        DataType.GEOPOLYGON,
        DataType.MARKUP,
        DataType.UUID,
        DataType.ARRAY_FLOAT,
        DataType.ARRAY_INT,
        DataType.ARRAY_STR,
        DataType.TREE_STR,
    ): ANY_TYPE,
}


class DialectStyle(Enum):
    simple = 'simple'
    multiline = 'multiline'
    split_version = 'split_version'


class StyledDialect(NamedTuple):
    simple: str
    multiline: str
    split_version: str

    def for_style(self, item):
        if isinstance(item, DialectStyle):
            item = item.name
        if isinstance(item, str):
            return getattr(self, item)


HIDDEN_DIALECTS: set[DialectCombo] = set()
ANY_DIALECTS = {
    *ClickHouseDialect.CLICKHOUSE.to_list(),
    *PostgreSQLDialect.NON_COMPENG_POSTGRESQL.to_list(),
    *MySQLDialect.MYSQL.to_list(),
    *MssqlDialect.MSSQLSRV.to_list(),
    *OracleDialect.ORACLE.to_list(),
    *MetricaDialect.METRIKAAPI.to_list(),
    *YqlDialect.YDB.to_list(),
}


HUMAN_DIALECTS = {
    ClickHouseDialect.CLICKHOUSE: StyledDialect(
        '`ClickHouse`',
        '`ClickHouse`',
        '`ClickHouse`',
    ),
    ClickHouseDialect.CLICKHOUSE_21_8: StyledDialect(
        '`ClickHouse 21.8`',
        '`ClickHouse`<br/>`21.8`',
        _('`ClickHouse` version `21.8`'),
    ),
    ClickHouseDialect.CLICKHOUSE_22_10: StyledDialect(
        '`ClickHouse 22.10`',
        '`ClickHouse`<br/>`22.10`',
        _('`ClickHouse` version `22.10`'),
    ),
    PostgreSQLDialect.POSTGRESQL: StyledDialect(
        '`PostgreSQL`',
        '`PostgreSQL`',
        '`PostgreSQL`',
    ),
    PostgreSQLDialect.POSTGRESQL_9_3: StyledDialect(
        '`PostgreSQL 9.3`',
        '`PostgreSQL`<br/>`9.3`',
        _('`PostgreSQL` version `9.3`'),
    ),
    PostgreSQLDialect.POSTGRESQL_9_4: StyledDialect(
        '`PostgreSQL 9.4`',
        '`PostgreSQL`<br/>`9.4`',
        _('`PostgreSQL` version `9.4`'),
    ),
    MySQLDialect.MYSQL: StyledDialect(
        '`MySQL`',
        '`MySQL`',
        '`MySQL`',
    ),
    MySQLDialect.MYSQL_5_6: StyledDialect(
        '`MySQL 5.6`',
        '`MySQL`<br/>`5.6`',
        _('`MySQL` version `5.6`'),
    ),
    MySQLDialect.MYSQL_5_7: StyledDialect(
        '`MySQL 5.7`',
        '`MySQL`<br/>`5.7`',
        _('`MySQL` version `5.7`'),
    ),
    MySQLDialect.MYSQL_8_0_12: StyledDialect(
        '`MySQL 8.0.12`',
        '`MySQL`<br/>`8.0.12`',
        _('`MySQL` version `8.0.12`'),
    ),
    MssqlDialect.MSSQLSRV_14_0: StyledDialect(
        '`Microsoft SQL Server 2017 (14.0)`',
        '`Microsoft`<br/>`SQL Server 2017`<br/>`(14.0)`',
        _('`Microsoft SQL Server` version `2017 (14.0)`'),
    ),
    OracleDialect.ORACLE_12_0: StyledDialect(
        '`Oracle Database 12c (12.1)`',
        '`Oracle`<br/>`Database 12c`<br/>`(12.1)`',
        _('`Oracle Database` version `12c (12.1)`'),
    ),
    MetricaDialect.METRIKAAPI: StyledDialect(
        '`Yandex Metrica`',
        '`Yandex Metrica`',
        '`Yandex Metrica`',
    ),
    D.SQLITE: StyledDialect(
        '`SQLite`',
        '`SQLite`',
        '`SQLite`',
    ),
    YqlDialect.YDB: StyledDialect(
        '`YDB`',
        '`YDB`<br/>(`YQL`)',
        '`YDB` (`YQL`)',
    ),
    YqlDialect.YQ: StyledDialect(
        '`YQ`',
        '`YQ`',
        '`YQ`',
    ),
    YqlDialect.YQL: StyledDialect(
        '`YQL`',
        '`YQL`',
        '`YQL`',
    ),
}

SIGNATURE_TITLE_STANDARD = _('Standard')
SIGNATURE_TITLE_EXTENDED = _('Extended')
SIGNATURE_DESC_EXTENDED_HEADER = _('More info:')


def register_any_dialects(any_dialects: frozenset[DialectCombo]) -> None:
    ANY_DIALECTS.update(any_dialects)


def register_human_dialects(human_dialects: dict[DialectCombo, StyledDialect]) -> None:
    HUMAN_DIALECTS.update(human_dialects)
