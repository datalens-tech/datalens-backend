"""Template-enabled ``_sys.user_id`` end-to-end (the RLS-shaped path).

The existing ``_sys.user_id`` tests (in ``dl_api_lib``) only exercise the parameter
inside a *formula*. This file covers the security-sensitive case where
``_sys.user_id`` is declared as a ``template_enabled`` parameter and substituted into a
**prepared source** (a parametrized sub-select). It verifies that the server-resolved
value lands in the rendered SQL, that a ``value_constraint`` validates the injected
value, and that a client-supplied value is still rejected in template mode.
"""

from http import HTTPStatus
from typing import ClassVar

import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import (
    Dataset,
    DataSource,
    RegexParameterValueConstraint,
    ResultField,
    StringParameterValue,
)
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_constants.enums import RawSQLLevel
from dl_core.connectors.settings.base import ConnectorSettings
from dl_testing.constants import TEST_USER_ID

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_SUBSELECT
from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse_tests.db.api.base import ClickHouseDataApiTestBase


class TestSysUserIdInSourceTemplate(ClickHouseDataApiTestBase):
    # Templating requires `RawSQLLevel.template` on the connection plus
    # `ENABLE_DATASOURCE_TEMPLATE` on the connector settings.
    raw_sql_level: ClassVar[RawSQLLevel | None] = RawSQLLevel.template

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[str, ConnectorSettings]:
        return {
            CONNECTION_TYPE_CLICKHOUSE.value: ClickHouseConnectorSettings(
                ENABLE_DATASOURCE_TEMPLATE=True,
            ),
        }

    def _make_dataset(self, control_api: SyncHttpDatasetApiV1, saved_connection_id: str) -> Dataset:
        """A dataset whose sub-select source embeds ``{{_sys.user_id}}``.

        The parameter is declared (so the template validates) with a constraint that the
        resolved request user id satisfies; the source schema is discovered by rendering
        the template with the parameter's ``default_value``.
        """
        ds = Dataset(template_enabled=True)
        ds.result_schema["_sys.user_id"] = ResultField(
            title="_sys.user_id",
            cast=StringParameterValue.type,
            value_constraint=RegexParameterValueConstraint(pattern=".+"),
            default_value=StringParameterValue(value="anon"),
            template_enabled=True,
        )
        ds.sources["source_1"] = DataSource(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_SUBSELECT.name,
            parameters=dict(subsql="SELECT '{{_sys.user_id}}' AS who"),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset
        assert not ds.component_errors.items, ds.component_errors
        return ds

    def test_sys_user_id_resolved_into_subselect_template(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_connection_id: str,
    ) -> None:
        ds = self._make_dataset(control_api, saved_connection_id)

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="who")],
            limit=1,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        # The server injects the request user id into the prepared source, overriding the
        # stored default_value; the constraint validated the injected value.
        assert get_data_rows(result_resp)[0][0] == TEST_USER_ID

    def test_sys_user_id_rejects_client_supplied_value_in_template_mode(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_connection_id: str,
    ) -> None:
        ds = self._make_dataset(control_api, saved_connection_id)

        parameter = ds.find_field(title="_sys.user_id")
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="who")],
            parameters=[parameter.parameter_value("hacked")],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
        assert (
            result_resp.bi_status_code
            == "ERR.DS_API.SOURCE_CONFIG.PARAMETER_VALUE_INVALID.SYSTEM_PARAMETER_NOT_SETTABLE"
        )
