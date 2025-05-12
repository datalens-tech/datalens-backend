import http
import json
import typing

import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_client.dsmaker.primitives import (
    Dataset,
    DataSource,
    ParameterValue,
    RegexParameterValueConstraint,
    ResultField,
    StringParameterValue,
)
from dl_api_lib.enums import DatasetAction
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

    @pytest.fixture(name="parameter_fields")
    def fixture_parameter_fields(self, sample_table: DbTable) -> dict[str, ResultField]:
        raise NotImplementedError

    @pytest.fixture(name="invalid_parameter_values")
    def fixture_invalid_parameter_values(self) -> dict[str, ParameterValue]:
        raise NotImplementedError

    @pytest.fixture(name="failed_constraint_parameter_values")
    def fixture_failed_constraint_parameter_values(self) -> dict[str, ParameterValue]:
        raise NotImplementedError

    @pytest.fixture(name="datasource")
    def fixture_datasource(self, saved_connection_id: str) -> DataSource:
        raise NotImplementedError

    @pytest.fixture(name="invalid_datasource")
    def fixture_invalid_datasource(self, saved_connection_id: str) -> DataSource:
        raise NotImplementedError

    @pytest.fixture(name="parameter_fields_factory")
    def fixture_parameter_fields_factory(
        self,
        parameter_fields: dict[str, ResultField],
        invalid_parameter_values: dict[str, ParameterValue],
    ) -> ParameterFieldsFactoryProtocol:
        def parameter_fields_factory(
            template_enabled: bool = True,
            invalid_default_value: bool = False,
        ) -> dict[str, ResultField]:
            result: dict[str, ResultField] = parameter_fields.copy()

            if template_enabled:
                for field in result.values():
                    field.template_enabled = template_enabled

            if invalid_default_value:
                for field_name, field in result.items():
                    field.default_value = invalid_parameter_values[field_name]

            return result

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

    @pytest.fixture(name="parameter_fields")
    def fixture_parameter_fields(self, sample_table: DbTable) -> dict[str, ResultField]:
        return {
            "table_name": ResultField(
                title="table_name",
                cast=StringParameterValue.type,
                value_constraint=RegexParameterValueConstraint(pattern="table_.*"),
                default_value=StringParameterValue(value=sample_table.name),
            ),
        }

    @pytest.fixture(name="invalid_parameter_values")
    def fixture_invalid_parameter_values(self) -> dict[str, ParameterValue]:
        return {
            "table_name": StringParameterValue(value="table_name_invalid"),
        }

    @pytest.fixture(name="failed_constraint_parameter_values")
    def fixture_failed_constraint_parameter_values(self) -> dict[str, ParameterValue]:
        return {
            "table_name": StringParameterValue(value="failed_constraint_table_name"),
        }

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

    @pytest.fixture(name="invalid_datasource")
    def fixture_invalid_datasource(self, saved_connection_id: str) -> DataSource:
        return DataSource(
            connection_id=saved_connection_id,
            source_type=self.source_type.name,
            parameters=dict(subsql="SELECT * FROM {{invalid_parameter_name}}"),
        )


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

        assert response.status_code == http.HTTPStatus.BAD_REQUEST
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

        assert response.status_code == http.HTTPStatus.BAD_REQUEST
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

        assert response.status_code == http.HTTPStatus.BAD_REQUEST
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

        assert response.status_code == http.HTTPStatus.BAD_REQUEST
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

        assert response.status_code == http.HTTPStatus.BAD_REQUEST
        assert response.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"
        component_errors = response.json["dataset"]["component_errors"]["items"]

        assert len(component_errors) == 1
        assert component_errors[0]["type"] == "data_source"
        assert component_errors[0]["errors"][0]["code"] == "ERR.DS_API.SOURCE_CONFIG.CONNECTION_TEMPLATE_DISABLED"


class BaseTestDataApiSourceTemplate(BaseTestSourceTemplate):
    @pytest.fixture(scope="function")
    def saved_dataset(
        self,
        control_api: SyncHttpDatasetApiV1,
        dataset_factory: DatasetFactoryProtocol,
    ) -> typing.Generator[Dataset, None, None]:
        ds = dataset_factory()
        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset
        yield ds
        control_api.delete_dataset(dataset_id=ds.id)

    def test_default(
        self,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
    ) -> None:
        ds = saved_dataset
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
        )

        assert result_resp.status_code == http.HTTPStatus.OK, result_resp.json

    def test_invalid_template(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        invalid_datasource: DataSource,
    ) -> None:
        ds = saved_dataset
        ds.sources["source_1"].parameters = invalid_datasource.parameters

        ds = control_api.apply_updates(dataset=ds, fail_ok=True).dataset
        ds = control_api.save_dataset(dataset=ds, fail_ok=True).dataset

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.SOURCE_CONFIG.TEMPLATE_INVALID"

    def test_connection_template_disabled(
        self,
        control_api_sync_client: SyncHttpClientBase,
        data_api: SyncHttpDataApiV2,
        saved_connection_id: str,
        saved_dataset: Dataset,
    ) -> None:
        control_api_sync_client.put(
            f"/api/v1/connections/{saved_connection_id}",
            data=json.dumps({"raw_sql_level": dl_constants_enums.RawSQLLevel.subselect.value}),
            content_type="application/json",
        )

        ds = saved_dataset
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.SOURCE_CONFIG.CONNECTION_TEMPLATE_DISABLED"

    def test_parameter_template_disabled(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        parameter_fields: dict[str, ResultField],
    ) -> None:
        ds = saved_dataset

        for field_name in parameter_fields.keys():
            ds.result_schema[field_name].template_enabled = False

        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.SOURCE_CONFIG.TEMPLATE_INVALID"

    def test_update_value_constraint_ignored(
        self,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        parameter_fields: dict[str, ResultField],
        failed_constraint_parameter_values: dict[str, ParameterValue],
    ) -> None:
        ds = saved_dataset

        get_result_updates = []

        for field_name in parameter_fields.keys():
            original_field = ds.find_field(title=field_name)
            assert original_field.cast is not None
            get_result_updates.append(
                {
                    "action": DatasetAction.update_field.value,
                    "field": {
                        "guid": original_field.id,
                        "cast": original_field.cast.name,
                        "value_constraint": {"type": "null"},
                        "default_value": failed_constraint_parameter_values[field_name].value,
                    },
                }
            )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            updates=get_result_updates,
            fail_ok=True,
        )
        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.PARAMETER.INVALID_VALUE"

    def test_update_template_enabled_ignored(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        parameter_fields: dict[str, ResultField],
    ) -> None:
        ds = saved_dataset

        for field_name in parameter_fields.keys():
            ds.result_schema[field_name].template_enabled = False

        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset

        get_result_updates = []

        for field_name in parameter_fields.keys():
            original_field = ds.find_field(title=field_name)
            assert original_field.cast is not None
            get_result_updates.append(
                {
                    "action": DatasetAction.update_field.value,
                    "field": {
                        "guid": original_field.id,
                        "cast": original_field.cast.name,
                        "template_enabled": True,
                    },
                }
            )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            updates=get_result_updates,
            fail_ok=True,
        )

        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.SOURCE_CONFIG.TEMPLATE_INVALID"

    def test_update_default_value(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        parameter_fields: dict[str, ResultField],
        invalid_parameter_values: dict[str, ParameterValue],
    ) -> None:
        ds = saved_dataset

        get_result_updates = []

        for field_name in parameter_fields.keys():
            original_field = ds.find_field(title=field_name)
            assert original_field.cast is not None
            assert original_field.default_value is not None
            get_result_updates.append(
                {
                    "action": DatasetAction.update_field.value,
                    "field": {
                        "guid": original_field.id,
                        "cast": original_field.cast.name,
                        "default_value": original_field.default_value.value,
                    },
                },
            )
            ds.result_schema[field_name].default_value = invalid_parameter_values[field_name]

        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            updates=get_result_updates,
        )
        assert result_resp.status_code == http.HTTPStatus.OK, result_resp.json

    def test_update_default_value_invalid(
        self,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        parameter_fields: dict[str, ResultField],
        invalid_parameter_values: dict[str, ParameterValue],
    ) -> None:
        ds = saved_dataset

        get_result_updates = []

        for field_name in parameter_fields.keys():
            original_field = ds.find_field(title=field_name)
            assert original_field.cast is not None
            get_result_updates.append(
                {
                    "action": DatasetAction.update_field.value,
                    "field": {
                        "guid": original_field.id,
                        "cast": original_field.cast.name,
                        "default_value": invalid_parameter_values[field_name].value,
                    },
                },
            )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            updates=get_result_updates,
            fail_ok=True,
        )

        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"

        for field in invalid_parameter_values.values():
            assert field.value in result_resp.json["message"]

    def test_update_default_value_fails_constraint(
        self,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        parameter_fields: dict[str, ResultField],
        failed_constraint_parameter_values: dict[str, ParameterValue],
    ) -> None:
        ds = saved_dataset

        get_result_updates = []

        for field_name in parameter_fields.keys():
            original_field = ds.find_field(title=field_name)
            assert original_field.cast is not None
            get_result_updates.append(
                {
                    "action": DatasetAction.update_field.value,
                    "field": {
                        "guid": original_field.id,
                        "cast": original_field.cast.name,
                        "default_value": failed_constraint_parameter_values[field_name].value,
                    },
                }
            )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            updates=get_result_updates,
            fail_ok=True,
        )

        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.PARAMETER.INVALID_VALUE"

        for field in failed_constraint_parameter_values.values():
            assert field.value in result_resp.json["message"]

    def test_update_parameter_value(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        parameter_fields: dict[str, ResultField],
        invalid_parameter_values: dict[str, ParameterValue],
    ) -> None:
        ds = saved_dataset

        parameters = []

        for field_name in parameter_fields.keys():
            original_field = ds.find_field(title=field_name)
            assert original_field.default_value is not None
            parameters.append(original_field.parameter_value(original_field.default_value.value))
            original_field.default_value = invalid_parameter_values[field_name]

        ds = control_api.apply_updates(dataset=ds, fail_ok=True).dataset
        ds = control_api.save_dataset(dataset=ds, fail_ok=True).dataset

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            parameters=parameters,
        )
        assert result_resp.status_code == http.HTTPStatus.OK, result_resp.json

    def test_update_parameter_value_invalid(
        self,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        parameter_fields: dict[str, ResultField],
        invalid_parameter_values: dict[str, ParameterValue],
    ) -> None:
        ds = saved_dataset
        parameters = []

        for field_name in parameter_fields.keys():
            original_field = ds.find_field(title=field_name)
            parameters.append(original_field.parameter_value(invalid_parameter_values[field_name].value))

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            parameters=parameters,
            fail_ok=True,
        )

        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"

        for field in invalid_parameter_values.values():
            assert field.value in result_resp.json["message"]

    def test_update_parameter_value_fails_constraint(
        self,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        parameter_fields: dict[str, ResultField],
        failed_constraint_parameter_values: dict[str, ParameterValue],
    ) -> None:
        ds = saved_dataset

        parameters = []

        for field_name in parameter_fields.keys():
            original_field = ds.find_field(title=field_name)
            parameters.append(original_field.parameter_value(failed_constraint_parameter_values[field_name].value))

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            parameters=parameters,
            fail_ok=True,
        )

        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.SOURCE_CONFIG.PARAMETER_VALUE_INVALID"

        for field in failed_constraint_parameter_values.values():
            assert field.value in result_resp.json["message"]
