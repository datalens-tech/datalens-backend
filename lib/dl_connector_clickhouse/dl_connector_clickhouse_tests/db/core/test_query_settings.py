"""ClickHouse core tests for the per-dataset `query_settings` feature.

Exercises `ConnectionClickhouse.validate_query_settings` and the adapter end-to-end
against a real connection, under each connector-settings configuration that matters.
"""

from collections.abc import Callable

import pytest

from dl_constants import RawSQLLevel
from dl_core.connection_executors import (
    AsyncConnExecutorBase,
    ConnExecutorQuery,
)
import dl_core.exc as dl_core_exc

from dl_connector_clickhouse.core.clickhouse.settings import (
    ClickHouseConnectorSettings,
    ClickHouseQuerySettingsSettings,
)
from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse_tests.db.core.base import BaseClickHouseTestClass


class TestClickHouseQuerySettingsInsufficientRawSqlLevelRejected(BaseClickHouseTestClass):
    """`ENABLED=True` but the connection is below the subselect SQL security level."""

    raw_sql_level = None
    connection_settings = ClickHouseConnectorSettings(
        QUERY_SETTINGS=ClickHouseQuerySettingsSettings(ENABLED=True),
    )

    def test_validate_rejects(self, saved_connection: ConnectionClickhouse) -> None:
        with pytest.raises(dl_core_exc.QuerySettingsNotSupportedError):
            saved_connection.validate_query_settings({"max_threads": "4"})


class TestClickHouseQuerySettingsDisabledOnConnectorRejected(BaseClickHouseTestClass):
    """SQL security level is fine, but the connector-level `ENABLED=False` (the default)."""

    raw_sql_level = RawSQLLevel.subselect
    connection_settings = ClickHouseConnectorSettings(
        QUERY_SETTINGS=ClickHouseQuerySettingsSettings(ENABLED=False),
    )

    def test_validate_rejects(self, saved_connection: ConnectionClickhouse) -> None:
        with pytest.raises(dl_core_exc.QuerySettingsNotSupportedError):
            saved_connection.validate_query_settings({"max_threads": "4"})


class TestClickHouseQuerySettingsForbiddenRejected(BaseClickHouseTestClass):
    """An FORBIDDEN name (e.g. `readonly`) is rejected even when the feature is on."""

    raw_sql_level = RawSQLLevel.subselect
    connection_settings = ClickHouseConnectorSettings(
        QUERY_SETTINGS=ClickHouseQuerySettingsSettings(ENABLED=True),
    )

    def test_validate_rejects(self, saved_connection: ConnectionClickhouse) -> None:
        with pytest.raises(dl_core_exc.QuerySettingForbiddenError):
            saved_connection.validate_query_settings({"readonly": "0"})


class TestClickHouseQuerySettingsNotInWhitelistRejected(BaseClickHouseTestClass):
    """With a restricted `ALLOWED` set, names outside it are rejected."""

    raw_sql_level = RawSQLLevel.subselect
    connection_settings = ClickHouseConnectorSettings(
        QUERY_SETTINGS=ClickHouseQuerySettingsSettings(
            ENABLED=True,
            ALLOWED=frozenset({"max_threads"}),
        ),
    )

    def test_validate_rejects(self, saved_connection: ConnectionClickhouse) -> None:
        with pytest.raises(dl_core_exc.QuerySettingNotAllowedError):
            saved_connection.validate_query_settings({"max_block_size": "1024"})


class TestClickHouseQuerySettingsApplied(BaseClickHouseTestClass):
    """End-to-end: the CH adapter sends the setting and the DB applies it."""

    raw_sql_level = RawSQLLevel.subselect
    connection_settings = ClickHouseConnectorSettings(
        QUERY_SETTINGS=ClickHouseQuerySettingsSettings(ENABLED=True),
    )

    @pytest.mark.asyncio
    async def test_adapter_applies_query_setting(
        self,
        async_conn_executor_factory: Callable[[], AsyncConnExecutorBase],
    ) -> None:
        executor = async_conn_executor_factory()
        try:
            result = await executor.execute(
                ConnExecutorQuery(
                    query="SELECT value FROM system.settings WHERE name = 'max_threads'",
                    query_settings={"max_threads": "7"},
                )
            )
            rows = [row async for chunk in result.result for row in chunk]
        finally:
            await executor.close()

        assert rows, "Empty response — query did not return the setting value"
        assert str(rows[0][0]) == "7", f"max_threads was not applied (got {rows[0][0]!r})"
