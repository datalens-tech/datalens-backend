import copy
import http
import json
from typing import (
    Any,
    ClassVar,
    Generator,
    Protocol,
    Sequence,
)

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


class ParameterFieldsFactoryProtocol(Protocol):
    def __call__(
        self,
        template_enabled: bool = True,
        invalid_default_value: bool = False,
    ) -> dict[str, ResultField]:
        pass


class DatasetFactoryProtocol(Protocol):
    def __call__(
        self,
        dataset_template_enabled: bool = True,
        parameters_template_enabled: bool = True,
        parameters_invalid_default_value: bool = False,
    ) -> Dataset:
        pass


class BaseTestSourceTemplate(ConnectionTestBase):
    source_type: ClassVar[DataSourceType]
    raw_sql_level = dl_constants_enums.RawSQLLevel.template
    connector_enable_datasource_template: ClassVar[bool] = True

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        settings = self.conn_settings_cls(  # type: ignore
            ENABLE_DATASOURCE_TEMPLATE=self.connector_enable_datasource_template,
        )
        return {self.conn_type: settings}

    @pytest.fixture(name="parameter_fields")
    def fixture_parameter_fields(self) -> dict[str, ResultField]:
        raise NotImplementedError

    @pytest.fixture(name="invalid_parameter_values")
    def fixture_invalid_parameter_values(self) -> dict[str, ParameterValue]:
        raise NotImplementedError

    @pytest.fixture(name="failed_constraint_parameter_values")
    def fixture_failed_constraint_parameter_values(self) -> dict[str, ParameterValue]:
        raise NotImplementedError

    @pytest.fixture(name="datasource_parameters")
    def fixture_datasource_parameters(self) -> dict[str, str]:
        raise NotImplementedError

    @pytest.fixture(name="datasource")
    def fixture_datasource(
        self,
        saved_connection_id: str,
        datasource_parameters: dict[str, str],
    ) -> DataSource:
        return DataSource(
            connection_id=saved_connection_id,
            source_type=self.source_type.name,
            parameters=datasource_parameters,
        )

    @pytest.fixture(name="invalid_datasource_parameters")
    def fixture_invalid_datasource_parameters(self) -> dict[str, str]:
        raise NotImplementedError

    @pytest.fixture(name="invalid_datasource")
    def fixture_invalid_datasource(
        self,
        saved_connection_id: str,
        invalid_datasource_parameters: dict[str, str],
    ) -> DataSource:
        return DataSource(
            connection_id=saved_connection_id,
            source_type=self.source_type.name,
            parameters=invalid_datasource_parameters,
        )

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
            result: dict[str, ResultField] = copy.deepcopy(parameter_fields)

            if template_enabled:
                for field in result.values():
                    field.template_enabled = template_enabled

            if invalid_default_value:
                for parameter_name, parameter_value in invalid_parameter_values.items():
                    result[parameter_name].default_value = parameter_value

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


class BaseTableTestSourceTemplate(BaseTestSourceTemplate):
    conn_settings_cls: ClassVar[ConnectorSettingsBase]
    table_name_pattern: ClassVar[str] = "table_.*"
    invalid_table_name: ClassVar[str] = "table_invalid"
    failed_constraint_table_name: ClassVar[str] = "failed_constraint_table_name"

    @pytest.fixture(name="table_name")
    def fixture_table_name(self, sample_table: DbTable) -> str:
        return sample_table.name

    @pytest.fixture(name="parameter_fields")
    def fixture_parameter_fields(self, table_name: str) -> dict[str, ResultField]:  # type: ignore  # LSP violation for fixture
        return {
            "table_name": ResultField(
                title="table_name",
                cast=StringParameterValue.type,
                value_constraint=RegexParameterValueConstraint(pattern=self.table_name_pattern),
                default_value=StringParameterValue(value=table_name),
            ),
        }

    @pytest.fixture(name="invalid_parameter_values")
    def fixture_invalid_parameter_values(self) -> dict[str, ParameterValue]:
        return {
            "table_name": StringParameterValue(value=self.invalid_table_name),
        }

    @pytest.fixture(name="failed_constraint_parameter_values")
    def fixture_failed_constraint_parameter_values(self) -> dict[str, ParameterValue]:
        return {
            "table_name": StringParameterValue(value=self.failed_constraint_table_name),
        }

    @pytest.fixture(name="datasource_parameters")
    def fixture_datasource_parameters(self) -> dict[str, str]:
        return dict(table_name="{{table_name}}")

    @pytest.fixture(name="invalid_datasource_parameters")
    def fixture_invalid_datasource_parameters(self) -> dict[str, str]:
        return dict(table_name="{{invalid_parameter_name}}")


class BaseSubselectTestSourceTemplate(BaseTableTestSourceTemplate):
    conn_settings_cls: ClassVar[ConnectorSettingsBase]

    @pytest.fixture(name="datasource_parameters")
    def fixture_datasource_parameters(self) -> dict[str, str]:
        return dict(subsql="SELECT * FROM {{table_name}}")

    @pytest.fixture(name="invalid_datasource_parameters")
    def fixture_invalid_datasource_parameters(self) -> dict[str, str]:
        return dict(subsql="SELECT * FROM {{invalid_parameter_name}}")


class BaseTestControlApiSourceTemplate(BaseTestSourceTemplate):
    excluded_assert_parameters: ClassVar[Sequence[str]] = ["db_version"]

    def test_default(
        self,
        control_api: SyncHttpDatasetApiV1,
        dataset_factory: DatasetFactoryProtocol,
    ) -> None:
        ds = dataset_factory()

        original_datasource_parameters = ds.sources["source_1"].parameters.copy()

        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset

        for parameter_name in original_datasource_parameters.keys():
            if parameter_name in self.excluded_assert_parameters:
                continue

            assert ds.sources["source_1"].parameters[parameter_name] == original_datasource_parameters[parameter_name]

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
        assert component_errors[0]["errors"][0]["code"].startswith("ERR.DS_API")

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
        assert component_errors[0]["errors"][0]["code"].startswith("ERR.DS_API.DB")

    def test_changing_invalid_default_value_fixes_datasource(
        self,
        control_api: SyncHttpDatasetApiV1,
        dataset_factory: DatasetFactoryProtocol,
        parameter_fields_factory: ParameterFieldsFactoryProtocol,
    ) -> None:
        ds = dataset_factory(parameters_invalid_default_value=True)
        response = control_api.apply_updates(dataset=ds, fail_ok=True)

        assert response.status_code == http.HTTPStatus.BAD_REQUEST
        assert response.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"
        ds = response.dataset
        parameter_fields = parameter_fields_factory(
            template_enabled=True,
            invalid_default_value=False,
        )
        updates: list[dict[str, Any]] = []
        for parameter_name, parameter_field in parameter_fields.items():
            original_field = ds.find_field(title=parameter_name)
            assert original_field.cast is not None
            assert parameter_field.default_value is not None

            updates.append(
                {
                    "action": DatasetAction.update_field.value,
                    "field": {
                        "guid": original_field.id,
                        "cast": original_field.cast.name,
                        "default_value": parameter_field.default_value.value,
                        "template_enabled": parameter_field.template_enabled,
                    },
                }
            )

        original_datasource_parameters = ds.sources["source_1"].parameters.copy()

        ds = control_api.apply_updates(dataset=ds, updates=updates).dataset
        ds = control_api.save_dataset(dataset=ds).dataset

        for parameter_name in original_datasource_parameters.keys():
            if parameter_name in self.excluded_assert_parameters:
                continue

            assert ds.sources["source_1"].parameters[parameter_name] == original_datasource_parameters[parameter_name]

    def test_dataset_template_disabled_then_enabled_refreshes_templated_sources(
        self,
        control_api: SyncHttpDatasetApiV1,
        dataset_factory: DatasetFactoryProtocol,
    ) -> None:
        ds = dataset_factory(dataset_template_enabled=False)
        response = control_api.apply_updates(dataset=ds, fail_ok=True)

        assert response.status_code == http.HTTPStatus.BAD_REQUEST
        assert response.bi_status_code == "ERR.DS_API.VALIDATION.ERROR"

        ds = response.dataset

        updates = [
            {
                "action": DatasetAction.update_setting.value,
                "setting": {
                    "name": "template_enabled",
                    "value": True,
                },
            }
        ]
        ds = control_api.apply_updates(dataset=ds, updates=updates).dataset
        ds = control_api.save_dataset(dataset=ds).dataset


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
    field_names: ClassVar[Sequence[str]] = ["discount", "city"]

    @pytest.fixture(scope="function")
    def saved_dataset(
        self,
        control_api: SyncHttpDatasetApiV1,
        dataset_factory: DatasetFactoryProtocol,
    ) -> Generator[Dataset, None, None]:
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
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
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
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
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
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
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
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
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

        for parameter_name, parameter_value in failed_constraint_parameter_values.items():
            original_field = ds.find_field(title=parameter_name)
            assert original_field.cast is not None
            get_result_updates.append(
                {
                    "action": DatasetAction.update_field.value,
                    "field": {
                        "guid": original_field.id,
                        "cast": original_field.cast.name,
                        "value_constraint": {"type": "null"},
                        "default_value": parameter_value.value,
                    },
                }
            )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
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
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
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

        for parameter_name, parameter_value in invalid_parameter_values.items():
            original_field = ds.find_field(title=parameter_name)
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
            ds.result_schema[parameter_name].default_value = parameter_value

        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
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

        for parameter_name, parameter_value in invalid_parameter_values.items():
            original_field = ds.find_field(title=parameter_name)
            assert original_field.cast is not None
            get_result_updates.append(
                {
                    "action": DatasetAction.update_field.value,
                    "field": {
                        "guid": original_field.id,
                        "cast": original_field.cast.name,
                        "default_value": parameter_value.value,
                    },
                },
            )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
            updates=get_result_updates,
            fail_ok=True,
        )

        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code is not None
        assert result_resp.bi_status_code.startswith("ERR.DS_API.DB")

    def test_update_default_value_fails_constraint(
        self,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        failed_constraint_parameter_values: dict[str, ParameterValue],
    ) -> None:
        ds = saved_dataset

        get_result_updates = []

        for parameter_name, parameter_value in failed_constraint_parameter_values.items():
            original_field = ds.find_field(title=parameter_name)
            assert original_field.cast is not None
            get_result_updates.append(
                {
                    "action": DatasetAction.update_field.value,
                    "field": {
                        "guid": original_field.id,
                        "cast": original_field.cast.name,
                        "default_value": parameter_value.value,
                    },
                }
            )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
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

        for parameter_name, parameter_value in invalid_parameter_values.items():
            original_field = ds.find_field(title=parameter_name)
            assert original_field.default_value is not None
            parameters.append(original_field.parameter_value(original_field.default_value.value))
            original_field.default_value = parameter_value

        ds = control_api.apply_updates(dataset=ds, fail_ok=True).dataset
        ds = control_api.save_dataset(dataset=ds, fail_ok=True).dataset

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
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

        for parameter_name, parameter_value in invalid_parameter_values.items():
            original_field = ds.find_field(title=parameter_name)
            parameters.append(original_field.parameter_value(parameter_value.value))

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
            parameters=parameters,
            fail_ok=True,
        )

        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code is not None
        assert result_resp.bi_status_code.startswith("ERR.DS_API.DB")

    def test_update_parameter_value_fails_constraint(
        self,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        failed_constraint_parameter_values: dict[str, ParameterValue],
    ) -> None:
        ds = saved_dataset

        parameters = []

        for parameter_name, parameter_value in failed_constraint_parameter_values.items():
            original_field = ds.find_field(title=parameter_name)
            parameters.append(original_field.parameter_value(parameter_value.value))

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=field_name) for field_name in self.field_names],
            parameters=parameters,
            fail_ok=True,
        )

        assert result_resp.status_code == http.HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.SOURCE_CONFIG.PARAMETER_VALUE_INVALID"

        for field in failed_constraint_parameter_values.values():
            assert field.value in result_resp.json["message"]
