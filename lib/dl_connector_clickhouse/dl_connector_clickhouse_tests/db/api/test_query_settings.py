"""ClickHouse API tests for the per-dataset `query_settings` feature.

Each class pins a specific connection/connector-settings configuration and exercises dataset
save through the control API and, where relevant, an end-to-end query through the data API.

Semantics: saving invalid `query_settings` **does not** reject the save; instead the offending
data sources are flagged with a component error, so the dataset persists in an "invalid" state.
The data API then refuses to run queries against such a dataset at execution time.
"""

from http import HTTPStatus
from typing import (
    ClassVar,
    Optional,
)

import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_constants.enums import (
    ComponentType,
    RawSQLLevel,
)
from dl_core.connectors.settings.base import ConnectorSettings

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_SUBSELECT
from dl_connector_clickhouse.core.clickhouse.settings import (
    ClickHouseConnectorSettings,
    ClickHouseQuerySettingsSettings,
)
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse_tests.db.api.base import ClickHouseDataApiTestBase


def _assert_source_error(dataset: Dataset, expected_code: str) -> None:
    """Assert every data_source pack on the dataset contains a component error with the given code."""
    packs = [item for item in dataset.component_errors.items if item.type == ComponentType.data_source]
    assert packs, f"expected at least one data_source error pack on the saved dataset, got {dataset.component_errors!r}"
    for pack in packs:
        codes = [err.code for err in pack.errors]
        assert expected_code in codes, f"source {pack.id}: expected {expected_code!r} in {codes!r}"


class _BaseClickHouseQuerySettingsApiTest(ClickHouseDataApiTestBase):
    raw_sql_level: ClassVar[Optional[RawSQLLevel]] = RawSQLLevel.subselect
    expected_query_settings_enabled: ClassVar[bool] = True

    @pytest.fixture(scope="class")
    def query_settings_settings(self) -> ClickHouseQuerySettingsSettings:
        return ClickHouseQuerySettingsSettings(ENABLED=True)

    @pytest.fixture(scope="class")
    def connectors_settings(
        self,
        query_settings_settings: ClickHouseQuerySettingsSettings,
    ) -> dict[str, ConnectorSettings]:
        return {
            CONNECTION_TYPE_CLICKHOUSE.value: ClickHouseConnectorSettings(
                QUERY_SETTINGS=query_settings_settings,
            ),
        }

    def test_options_reflects_query_settings_enabled(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        response = control_api.client.get(f"/api/v1/datasets/{saved_dataset.id}/versions/draft").json
        assert response["options"]["query_settings_enabled"] is self.expected_query_settings_enabled


class TestClickHouseQuerySettingsSavePasses(_BaseClickHouseQuerySettingsApiTest):
    """`ENABLED=True` + subselect + default `ALLOWED=None` → valid settings round-trip and a data
    query actually applies the setting (verified by reading CH's own session settings)."""

    @pytest.fixture(scope="class")
    def dataset_params(self) -> dict:
        subsql = "SELECT value AS value FROM system.settings WHERE name = 'max_threads'"
        return dict(source_type=SOURCE_TYPE_CH_SUBSELECT.name, parameters=dict(subsql=subsql))

    def test_save_with_valid_query_settings_round_trips(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        saved_dataset.query_settings = {"max_threads": "4"}
        saved = control_api.save_dataset(saved_dataset).dataset
        assert not saved.component_errors.items, saved.component_errors
        loaded = control_api.get_dataset(saved.id).dataset
        assert loaded.query_settings == {"max_threads": "4"}

    def test_data_api_applies_query_setting(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
    ) -> None:
        saved_dataset.query_settings = {"max_threads": "7"}
        ds = control_api.save_dataset(saved_dataset).dataset

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="value")],
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        rows = get_data_rows(result_resp)
        assert rows, "Expected at least one row"
        assert str(rows[0][0]) == "7", f"max_threads was not applied (got {rows[0][0]!r})"


class _BaseSaveRecordsInvalidAndDataApiRejects(_BaseClickHouseQuerySettingsApiTest):
    """Shared behavior: save with invalid query_settings → 200 with component error on each source,
    then a data API call → 400 with the matching `bi_status_code`."""

    invalid_query_settings: ClassVar[dict[str, str]]
    expected_bi_status_code: ClassVar[str]

    def test_save_records_component_error(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        saved_dataset.query_settings = self.invalid_query_settings
        response = control_api.save_dataset(saved_dataset)
        assert response.status_code == HTTPStatus.OK, response.json
        _assert_source_error(response.dataset, self.expected_bi_status_code)

        reloaded = control_api.get_dataset(saved_dataset.id).dataset
        _assert_source_error(reloaded, self.expected_bi_status_code)
        assert reloaded.query_settings == self.invalid_query_settings

    def test_data_api_refuses_to_run(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
    ) -> None:
        saved_dataset.query_settings = self.invalid_query_settings
        ds = control_api.save_dataset(saved_dataset).dataset

        response = data_api.get_result(
            dataset=ds,
            fields=[ds.result_schema[0]],
            fail_ok=True,
        )
        assert response.status_code == HTTPStatus.BAD_REQUEST, response.json
        assert response.bi_status_code == self.expected_bi_status_code


class TestClickHouseQuerySettingsInvalidWhenFeatureOff(_BaseSaveRecordsInvalidAndDataApiRejects):
    """Default raw_sql_level=off and default connector settings → settings are recorded as invalid
    (QUERY_SETTINGS.NOT_SUPPORTED), data API refuses."""

    raw_sql_level: ClassVar[Optional[RawSQLLevel]] = RawSQLLevel.off
    expected_query_settings_enabled = False

    invalid_query_settings = {"max_threads": "4"}
    expected_bi_status_code = "ERR.DS_API.DS_CONFIG.QUERY_SETTINGS.NOT_SUPPORTED"


class TestClickHouseQuerySettingsInvalidWhenForbidden(_BaseSaveRecordsInvalidAndDataApiRejects):
    """A FORBIDDEN name (e.g. `readonly`) is recorded invalid even when the feature is on."""

    invalid_query_settings = {"readonly": "0"}
    expected_bi_status_code = "ERR.DS_API.DS_CONFIG.QUERY_SETTINGS.FORBIDDEN"


class TestClickHouseQuerySettingsInvalidWhenNotInWhitelist(_BaseSaveRecordsInvalidAndDataApiRejects):
    """With a restricted `ALLOWED` set, names outside it are recorded invalid."""

    invalid_query_settings = {"max_block_size": "1024"}
    expected_bi_status_code = "ERR.DS_API.DS_CONFIG.QUERY_SETTINGS.NOT_ALLOWED"

    @pytest.fixture(scope="class")
    def query_settings_settings(self) -> ClickHouseQuerySettingsSettings:
        return ClickHouseQuerySettingsSettings(
            ENABLED=True,
            ALLOWED=frozenset({"max_threads"}),
        )
