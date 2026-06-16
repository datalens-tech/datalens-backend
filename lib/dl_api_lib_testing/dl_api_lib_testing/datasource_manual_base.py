from typing import ClassVar

import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import (
    Dataset,
    RegexParameterValueConstraint,
    ResultField,
    StringParameterValue,
)
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_constants import (
    DataSourceType,
    RawSQLLevel,
)
from dl_core.connectors.settings.base import ConnectorSettings
from dl_core_testing.database import DbTable


class BaseTestDatasourceManual(ConnectionTestBase):
    """Generic manual-flag coverage that works for any source kind.

    Subclasses bind ``source_type`` and ``conn_settings_cls`` and provide a
    ``parameters`` fixture that produces a *valid* parameters dict for that source
    (without the ``manual`` key). The default ``raw_sql_level`` is high enough to
    allow manual user-input sources.
    """

    source_type: ClassVar[DataSourceType]
    conn_settings_cls: ClassVar[type[ConnectorSettings]]

    raw_sql_level = RawSQLLevel.template

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[str, ConnectorSettings]:
        return {self.conn_type.value: self.conn_settings_cls()}

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        raise NotImplementedError

    def _make_dataset(self, saved_connection_id: str, parameters: dict) -> Dataset:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=self.source_type.name,
            parameters=parameters,
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        return ds

    def _make_saved_dataset(self, saved_connection_id: str, parameters: dict) -> Dataset:
        ds = Dataset()
        source = ds.source(
            connection_id=saved_connection_id,
            source_type=self.source_type.name,
            parameters=parameters,
        )
        source.created_ = True
        source.prepare()
        ds.sources["source_1"] = source

        avatar = source.avatar()
        avatar.created_ = True
        avatar.prepare()
        ds.source_avatars["avatar_1"] = avatar
        return ds

    def test_validate_assigns_manual_default(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, parameters)
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert isinstance(ds.sources["source_1"].parameters["manual"], bool)
        assert ds.sources["source_1"].valid is True

    def test_save_assigns_manual_default(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        parameters: dict,
    ) -> None:
        ds = self._make_saved_dataset(saved_connection_id, parameters)
        ds = control_api.save_dataset(dataset=ds, fail_ok=False).dataset
        assert isinstance(ds.sources["source_1"].parameters["manual"], bool)
        assert ds.sources["source_1"].valid is True

    def test_validate_preserves_manual_true(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, dict(parameters, manual=True))
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].parameters["manual"] is True
        assert ds.sources["source_1"].valid is True

    def test_save_preserves_manual_true(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        parameters: dict,
    ) -> None:
        ds = self._make_saved_dataset(saved_connection_id, dict(parameters, manual=True))
        ds = control_api.save_dataset(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].parameters["manual"] is True
        assert ds.sources["source_1"].valid is True


class BaseTestDatasourceManualTable(BaseTestDatasourceManual):
    """Adds table-specific coverage: discoverable default, missing table, templated table.

    Subclasses additionally provide:

    - ``missing_table_parameters`` — parameters with a table_name that doesn't exist
      in the connected DB.
    - ``templated_table_parameters`` — parameters where ``table_name`` references a
      dataset parameter (``{{table_name}}``); the dataset must be created with that
      parameter defined.
    """

    @pytest.fixture(name="missing_table_parameters")
    def fixture_missing_table_parameters(self, sample_table: DbTable) -> dict:
        raise NotImplementedError

    @pytest.fixture(name="templated_table_parameters")
    def fixture_templated_table_parameters(self, sample_table: DbTable) -> dict:
        raise NotImplementedError

    def _make_templated_dataset(
        self,
        saved_connection_id: str,
        sample_table: DbTable,
        templated_parameters: dict,
    ) -> Dataset:
        ds = Dataset(template_enabled=True)
        ds.result_schema["table_name"] = ResultField(
            title="table_name",
            cast=StringParameterValue.type,
            value_constraint=RegexParameterValueConstraint(pattern=".*"),
            default_value=StringParameterValue(value=sample_table.name),
            template_enabled=True,
        )
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=self.source_type.name,
            parameters=templated_parameters,
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        return ds

    def test_default_manual_is_false_for_table(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, parameters)
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].parameters["manual"] is False
        assert ds.sources["source_1"].valid is True

    def test_manual_false_marks_invalid_for_nonexistent_table(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        missing_table_parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, dict(missing_table_parameters, manual=False))
        resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.dataset.sources["source_1"].valid is False

    def test_manual_false_marks_valid_for_existing_table(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, dict(parameters, manual=False))
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].valid is True

    def test_manual_true_preserves_valid_for_nonexistent_table(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        missing_table_parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, dict(missing_table_parameters, manual=True))
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].parameters["manual"] is True
        assert ds.sources["source_1"].valid is True

    def test_toggle_manual_false_to_true_resets_valid(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        missing_table_parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, dict(missing_table_parameters, manual=False))
        resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.dataset.sources["source_1"].valid is False

        ds.sources["source_1"].parameters["manual"] = True
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].valid is True

    def test_default_manual_is_true_for_templated_table(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table: DbTable,
        templated_table_parameters: dict,
    ) -> None:
        ds = self._make_templated_dataset(saved_connection_id, sample_table, templated_table_parameters)
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].parameters["manual"] is True
        assert ds.sources["source_1"].valid is True

    def test_manual_false_marks_invalid_for_templated_table(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table: DbTable,
        templated_table_parameters: dict,
    ) -> None:
        ds = self._make_templated_dataset(
            saved_connection_id,
            sample_table,
            dict(templated_table_parameters, manual=False),
        )
        resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.dataset.sources["source_1"].valid is False


class BaseTestDatasourceManualSubselect(BaseTestDatasourceManual):
    """Adds subselect-specific coverage: default is manual=True, manual=False rejected."""

    def test_default_manual_is_true_for_subselect(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, parameters)
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].parameters["manual"] is True
        assert ds.sources["source_1"].valid is True

    def test_manual_false_marks_invalid_for_subselect(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, dict(parameters, manual=False))
        resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.dataset.sources["source_1"].valid is False


class BaseTestDatasourceManualRawSqlLevelOff(ConnectionTestBase):
    """``raw_sql_level=off``: manual=True table sources are rejected on the connection.

    Subclasses bind ``source_type`` (a table source), ``conn_settings_cls``, and
    provide a ``parameters`` fixture for an existing table.
    """

    source_type: ClassVar[DataSourceType]
    conn_settings_cls: ClassVar[type[ConnectorSettings]]

    raw_sql_level = RawSQLLevel.off

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[str, ConnectorSettings]:
        return {self.conn_type.value: self.conn_settings_cls()}

    @pytest.fixture(name="parameters")
    def fixture_parameters(self, sample_table: DbTable) -> dict:
        raise NotImplementedError

    def _make_dataset(self, saved_connection_id: str, parameters: dict) -> Dataset:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=self.source_type.name,
            parameters=parameters,
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        return ds

    def test_manual_true_table_rejected_at_raw_sql_level_off(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, dict(parameters, manual=True))
        resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.dataset.sources["source_1"].valid is False

    def test_manual_false_existing_table_ok_at_raw_sql_level_off(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, dict(parameters, manual=False))
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].valid is True

    def test_default_manual_for_non_templated_table_is_false(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        parameters: dict,
    ) -> None:
        ds = self._make_dataset(saved_connection_id, parameters)
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].parameters["manual"] is False
        assert ds.sources["source_1"].valid is True
