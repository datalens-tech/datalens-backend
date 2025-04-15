import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import (
    Dataset,
    StringParameterValue,
)
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_configs.connectors_settings import ConnectorSettingsBase
import dl_constants.enums as dl_constants_enums
from dl_constants.enums import ConnectionType
from dl_core_testing.database import DbTable

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_SUBSELECT
from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE


class TestSourceTemplate(DefaultApiTestBase):
    raw_sql_level = dl_constants_enums.RawSQLLevel.template

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        return {CONNECTION_TYPE_CLICKHOUSE: ClickHouseConnectorSettings(ENABLE_DATASOURCE_TEMPLATE=True)}

    def test_default(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table: DbTable,
    ):
        table_name = sample_table.name
        ds = Dataset(template_enabled=True)
        parameter = StringParameterValue(value=table_name)
        ds.result_schema["table_name"] = ds.field(
            cast=parameter.type,
            default_value=parameter,
            value_constraint=None,
            template_enabled=True,
        )
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_SUBSELECT.name,
            parameters=dict(subsql="SELECT * FROM {{table_name}}"),
        )

        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset

        assert ds.sources["source_1"].parameters["subsql"] == "SELECT * FROM {{table_name}}"
        assert len(ds.sources["source_1"].raw_schema) == 21  # raw schema is properly fetched

    def test_dataset_template_disabled(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table: DbTable,
    ):
        table_name = sample_table.name
        ds = Dataset(template_enabled=False)
        parameter = StringParameterValue(value=table_name)
        ds.result_schema["table_name"] = ds.field(
            cast=parameter.type,
            default_value=parameter,
            value_constraint=None,
            template_enabled=True,
        )
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_SUBSELECT.name,
            parameters=dict(subsql="SELECT * FROM {{table_name}}"),
        )

        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        response = control_api.apply_updates(dataset=ds, fail_ok=True)

        assert response.status_code == 400
        assert response.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"
        component_errors = response.json["dataset"]["component_errors"]["items"]

        assert len(component_errors) == 1
        assert component_errors[0]["type"] == "data_source"
        assert component_errors[0]["errors"][0]["code"] == "ERR.DS_API.DB.INVALID_QUERY"

    def test_parameter_template_disabled(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table: DbTable,
    ):
        table_name = sample_table.name
        ds = Dataset(template_enabled=True)
        parameter = StringParameterValue(value=table_name)
        ds.result_schema["table_name"] = ds.field(
            cast=parameter.type,
            default_value=parameter,
            value_constraint=None,
        )
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_SUBSELECT.name,
            parameters=dict(subsql="SELECT * FROM {{table_name}}"),
        )

        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        response = control_api.apply_updates(dataset=ds, fail_ok=True)

        assert response.status_code == 400
        assert response.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"
        component_errors = response.json["dataset"]["component_errors"]["items"]

        assert len(component_errors) == 1
        assert component_errors[0]["type"] == "data_source"
        assert component_errors[0]["errors"][0]["code"] == "ERR.DS_API.SOURCE_CONFIG.TEMPLATE_INVALID"

    def test_default_value_no_datasource(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
    ):
        ds = Dataset(template_enabled=True)
        parameter = StringParameterValue(value="invalid_table_name")
        ds.result_schema["table_name"] = ds.field(
            cast=parameter.type,
            default_value=parameter,
            value_constraint=None,
            template_enabled=True,
        )
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_SUBSELECT.name,
            parameters=dict(subsql="SELECT * FROM {{table_name}}"),
        )

        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        response = control_api.apply_updates(dataset=ds, fail_ok=True)

        assert response.status_code == 400
        assert response.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"
        component_errors = response.json["dataset"]["component_errors"]["items"]

        assert len(component_errors) == 1
        assert component_errors[0]["type"] == "data_source"
        assert component_errors[0]["errors"][0]["code"] == "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"


class TestSettingsDisabledSourceTemplate(DefaultApiTestBase):
    raw_sql_level = dl_constants_enums.RawSQLLevel.template

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        return {CONNECTION_TYPE_CLICKHOUSE: ClickHouseConnectorSettings(ENABLE_DATASOURCE_TEMPLATE=False)}

    def test_default(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table: DbTable,
    ):
        table_name = sample_table.name
        ds = Dataset(template_enabled=True)
        parameter = StringParameterValue(value=table_name)
        ds.result_schema["table_name"] = ds.field(
            cast=parameter.type,
            default_value=parameter,
            value_constraint=None,
            template_enabled=True,
        )
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_SUBSELECT.name,
            parameters=dict(subsql="SELECT * FROM {{table_name}}"),
        )

        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        response = control_api.apply_updates(dataset=ds, fail_ok=True)

        assert response.status_code == 400
        assert response.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"
        component_errors = response.json["dataset"]["component_errors"]["items"]

        assert len(component_errors) == 1
        assert component_errors[0]["type"] == "data_source"
        assert component_errors[0]["errors"][0]["code"] == "ERR.DS_API.SOURCE_CONFIG.CONNECTION_TEMPLATE_DISABLED"


class TestConnectionDisabledSourceTemplate(DefaultApiTestBase):
    raw_sql_level = dl_constants_enums.RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        return {CONNECTION_TYPE_CLICKHOUSE: ClickHouseConnectorSettings(ENABLE_DATASOURCE_TEMPLATE=True)}

    def test_default(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table: DbTable,
    ):
        table_name = sample_table.name
        ds = Dataset(template_enabled=True)
        parameter = StringParameterValue(value=table_name)
        ds.result_schema["table_name"] = ds.field(
            cast=parameter.type,
            default_value=parameter,
            value_constraint=None,
        )
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_SUBSELECT.name,
            parameters=dict(subsql="SELECT * FROM {table_name}"),
        )

        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        response = control_api.apply_updates(dataset=ds, fail_ok=True)

        assert response.status_code == 400
        assert response.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"
        component_errors = response.json["dataset"]["component_errors"]["items"]

        assert len(component_errors) == 1
        assert component_errors[0]["type"] == "data_source"
        assert component_errors[0]["errors"][0]["code"] == "ERR.DS_API.SOURCE_CONFIG.CONNECTION_TEMPLATE_DISABLED"
