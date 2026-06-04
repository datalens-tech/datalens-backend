import dl_connector_starrocks.core.adapters_base_starrocks as adapters_base_starrocks
from dl_connector_starrocks.core.constants import STARROCKS_SYSTEM_CATALOGS


def test_list_tables_query_excludes_all_system_schemas() -> None:
    """The SELECT FROM information_schema.tables query must exclude every
    schema declared in STARROCKS_SYSTEM_CATALOGS via a WHERE NOT IN list.
    """
    query = adapters_base_starrocks.StarRocksQueryConstructorMixin().get_list_tables_query(catalog="default_catalog")
    sql = str(query)

    assert "TABLE_SCHEMA NOT IN" in sql
    for schema in STARROCKS_SYSTEM_CATALOGS:
        assert f"'{schema}'" in sql, f"system schema {schema!r} missing from NOT IN list"
