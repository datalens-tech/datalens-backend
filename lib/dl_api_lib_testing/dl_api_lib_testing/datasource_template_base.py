import typing

import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import (
    Dataset,
    DataSource,
    ResultField,
    StringParameterValue,
)
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_configs.connectors_settings import ConnectorSettingsBase
import dl_constants.enums as dl_constants_enums
from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
)
from dl_core_testing.database import DbTable


class ParameterFieldsFactoryProtocol(typing.Protocol):
    def __call__(
        self,
        template_enabled: bool = True,
        invalid_default_value: bool = False,
    ) -> dict[str, ResultField]:
        pass


class DatasetFactoryProtocol(typing.Protocol):
    def __call__(
        self,
        dataset_template_enabled: bool = True,
        parameters_template_enabled: bool = True,
        parameters_invalid_default_value: bool = False,
    ) -> Dataset:
        pass


class BaseTestSourceTemplate(ConnectionTestBase):
    raw_sql_level = dl_constants_enums.RawSQLLevel.template
    connector_enable_datasource_template: typing.ClassVar[bool] = True

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        raise NotImplementedError

    @pytest.fixture(name="datasource")
    def fixture_datasource(self, saved_connection_id: str) -> DataSource:
        raise NotImplementedError

    @pytest.fixture(name="parameter_fields_factory")
    def fixture_parameter_fields_factory(
        self,
        sample_table: DbTable,
    ) -> ParameterFieldsFactoryProtocol:
        def parameter_fields_factory(
            template_enabled: bool = True,
            invalid_default_value: bool = False,
        ) -> dict[str, ResultField]:
            table_name = "invalid_table_name" if invalid_default_value else sample_table.name

            return {
                "table_name": ResultField(
                    cast=StringParameterValue.type,
                    default_value=StringParameterValue(value=table_name),
                    value_constraint=None,
                    template_enabled=template_enabled,
                )
            }

        return parameter_fields_factory

    @pytest.fixture(name="dataset_factory")
    def fixture_dataset_factory(
        self,
        parameter_fields_factory: ParameterFieldsFactoryProtocol,
        datasource: DataSource,
    ) -> DatasetFactoryProtocol:
        def dataset_factory(
            dataset_template_enabled: bool = True,
            parameters_template_enabled: bool = True,
            parameters_invalid_default_value: bool = False,
        ) -> Dataset:
            ds = Dataset(template_enabled=dataset_template_enabled)

            parameter_fields = parameter_fields_factory(
                template_enabled=parameters_template_enabled,
                invalid_default_value=parameters_invalid_default_value,
            )
            for field_name, field in parameter_fields.items():
                ds.result_schema[field_name] = field

            ds.sources["source_1"] = datasource
            ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

            return ds

        return dataset_factory


class BaseSubselectTestSourceTemplate(BaseTestSourceTemplate):
    source_type: typing.ClassVar[DataSourceType]
    conn_settings_cls: typing.ClassVar[ConnectorSettingsBase]

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        settings = self.conn_settings_cls(  # type: ignore
            ENABLE_DATASOURCE_TEMPLATE=self.connector_enable_datasource_template,
        )
        return {self.conn_type: settings}

    @pytest.fixture(name="datasource")
    def fixture_datasource(self, saved_connection_id: str) -> DataSource:
        return DataSource(
            connection_id=saved_connection_id,
            source_type=self.source_type.name,
            parameters=dict(subsql="SELECT * FROM {{table_name}}"),
        )

    @pytest.fixture(name="parameter_fields_factory")
    def fixture_parameter_fields_factory(
        self,
        sample_table: DbTable,
    ) -> ParameterFieldsFactoryProtocol:
        def parameter_fields_factory(
            template_enabled: bool = True,
            invalid_default_value: bool = False,
        ) -> dict[str, ResultField]:
            table_name = "invalid_table_name" if invalid_default_value else sample_table.name

            return {
                "table_name": ResultField(
                    cast=StringParameterValue.type,
                    default_value=StringParameterValue(value=table_name),
                    value_constraint=None,
                    template_enabled=template_enabled,
                )
            }

        return parameter_fields_factory


class BaseTestControlApiSourceTemplate(BaseTestSourceTemplate):
    def test_default(
        self,
        control_api: SyncHttpDatasetApiV1,
        dataset_factory: DatasetFactoryProtocol,
    ) -> None:
        ds = dataset_factory()

        original_datasource_parameters = ds.sources["source_1"].parameters.copy()

        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset

        assert ds.sources["source_1"].parameters == original_datasource_parameters
        assert len(ds.sources["source_1"].raw_schema) == 21

    def test_dataset_template_disabled(
        self,
        control_api: SyncHttpDatasetApiV1,
        dataset_factory: DatasetFactoryProtocol,
    ) -> None:
        ds = dataset_factory(dataset_template_enabled=False)

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
        dataset_factory: DatasetFactoryProtocol,
    ) -> None:
        ds = dataset_factory(parameters_template_enabled=False)

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
        dataset_factory: DatasetFactoryProtocol,
    ) -> None:
        ds = dataset_factory(parameters_invalid_default_value=True)
        response = control_api.apply_updates(dataset=ds, fail_ok=True)

        assert response.status_code == 400
        assert response.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"
        component_errors = response.json["dataset"]["component_errors"]["items"]

        assert len(component_errors) == 1
        assert component_errors[0]["type"] == "data_source"
        assert component_errors[0]["errors"][0]["code"] == "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"


class BaseTestControlApiSourceTemplateSettingsDisabled(BaseTestSourceTemplate):
    connector_enable_datasource_template = False

    def test_default(
        self,
        control_api: SyncHttpDatasetApiV1,
        dataset_factory: DatasetFactoryProtocol,
    ) -> None:
        ds = dataset_factory()
        response = control_api.apply_updates(dataset=ds, fail_ok=True)

        assert response.status_code == 400
        assert response.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"
        component_errors = response.json["dataset"]["component_errors"]["items"]

        assert len(component_errors) == 1
        assert component_errors[0]["type"] == "data_source"
        assert component_errors[0]["errors"][0]["code"] == "ERR.DS_API.SOURCE_CONFIG.CONNECTION_TEMPLATE_DISABLED"


class BaseTestControlApiSourceTemplateConnectionDisabled(BaseTestSourceTemplate):
    raw_sql_level = dl_constants_enums.RawSQLLevel.subselect

    def test_default(
        self,
        control_api: SyncHttpDatasetApiV1,
        dataset_factory: DatasetFactoryProtocol,
    ) -> None:
        ds = dataset_factory()
        response = control_api.apply_updates(dataset=ds, fail_ok=True)

        assert response.status_code == 400
        assert response.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"
        component_errors = response.json["dataset"]["component_errors"]["items"]

        assert len(component_errors) == 1
        assert component_errors[0]["type"] == "data_source"
        assert component_errors[0]["errors"][0]["code"] == "ERR.DS_API.SOURCE_CONFIG.CONNECTION_TEMPLATE_DISABLED"
