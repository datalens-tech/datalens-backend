from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import (
    Dataset,
    RegexParameterValueConstraint,
    ResultField,
    StringParameterValue,
)
import dl_api_lib_testing
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import RawSQLLevel

from dl_connector_clickhouse.core.clickhouse.constants import (
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)
from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings


class TestDatasourceManual(
    dl_api_lib_testing.BaseTableTestSourceTemplate,
    DefaultApiTestBase,
):
    source_type = SOURCE_TYPE_CH_TABLE
    conn_settings_cls = ClickHouseConnectorSettings

    def test_validate_adds_manual_when_not_passed(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset

        assert ds.sources["source_1"].parameters["manual"] is False
        assert ds.sources["source_1"].valid is True

    def test_save_adds_manual_when_not_passed(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        source = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )
        source.created_ = True
        source.prepare()
        ds.sources["source_1"] = source

        avatar = source.avatar()
        avatar.created_ = True
        avatar.prepare()
        ds.source_avatars["avatar_1"] = avatar

        ds = control_api.save_dataset(dataset=ds, fail_ok=False).dataset

        assert ds.sources["source_1"].parameters["manual"] is False
        assert ds.sources["source_1"].valid is True

    def test_validate_preserves_manual_when_passed(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
                manual=True,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset

        assert ds.sources["source_1"].parameters["manual"] is True
        assert ds.sources["source_1"].valid is True

    def test_save_preserves_manual_when_passed(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        source = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
                manual=True,
            ),
        )
        source.created_ = True
        source.prepare()
        ds.sources["source_1"] = source

        avatar = source.avatar()
        avatar.created_ = True
        avatar.prepare()
        ds.source_avatars["avatar_1"] = avatar

        ds = control_api.save_dataset(dataset=ds, fail_ok=False).dataset

        assert ds.sources["source_1"].parameters["manual"] is True
        assert ds.sources["source_1"].valid is True

    def test_manual_false_marks_invalid_for_nonexistent_table(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name="table_that_does_not_exist",
                manual=False,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

        resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.dataset.sources["source_1"].valid is False

    def test_manual_false_marks_valid_for_existing_table(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
                manual=False,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset

        assert ds.sources["source_1"].valid is True

    def test_default_manual_is_true_for_templated_table(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
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
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name="{{table_name}}",
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].parameters["manual"] is True
        assert ds.sources["source_1"].valid is True

    def test_manual_false_marks_invalid_for_templated_table(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
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
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name="{{table_name}}",
                manual=False,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

        resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.dataset.sources["source_1"].valid is False

    def test_default_manual_is_true_for_subselect(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_SUBSELECT.name,
            parameters=dict(
                subsql=f"select * from {sample_table.db.name}.{sample_table.name}",
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].parameters["manual"] is True
        assert ds.sources["source_1"].valid is True

    def test_manual_false_marks_invalid_for_subselect(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_SUBSELECT.name,
            parameters=dict(
                subsql=f"select * from {sample_table.db.name}.{sample_table.name}",
                manual=False,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

        resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.dataset.sources["source_1"].valid is False

    def test_manual_true_preserves_valid_for_nonexistent_table(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name="table_that_does_not_exist",
                manual=True,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset

        assert ds.sources["source_1"].parameters["manual"] is True
        assert ds.sources["source_1"].valid is True

    def test_toggle_manual_false_to_true_resets_valid(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name="table_that_does_not_exist",
                manual=False,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.dataset.sources["source_1"].valid is False

        ds.sources["source_1"].parameters["manual"] = True
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].valid is True


class TestDatasourceManualRawSqlLevelOff(DefaultApiTestBase):
    """Manual table sources require raw_sql_level >= subselect on the connection."""

    raw_sql_level = RawSQLLevel.off

    def test_manual_true_table_rejected_at_raw_sql_level_off(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
                manual=True,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

        resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.dataset.sources["source_1"].valid is False

    def test_manual_false_existing_table_ok_at_raw_sql_level_off(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
                manual=False,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].valid is True

    def test_default_manual_for_non_templated_table_is_false(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        sample_table,
    ) -> None:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id,
            source_type=SOURCE_TYPE_CH_TABLE.name,
            parameters=dict(
                db_name=sample_table.db.name,
                table_name=sample_table.name,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        assert ds.sources["source_1"].parameters["manual"] is False
        assert ds.sources["source_1"].valid is True
