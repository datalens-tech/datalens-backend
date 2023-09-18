from sqlalchemy_metrika_api.api_info.metrika import MetrikaApiCounterSource

from dl_core.us_connection_base import DataSourceTemplate
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from bi_connector_metrica.core.constants import SOURCE_TYPE_METRICA_API
from bi_connector_metrica.core.us_connection import MetrikaApiConnection

from bi_connector_metrica_tests.ext.core.base import BaseMetricaTestClass


class TestMetricaConnection(BaseMetricaTestClass, DefaultConnectionTestClass[MetrikaApiConnection]):
    def check_saved_connection(self, conn: MetrikaApiConnection, params: dict) -> None:
        pass

    def test_connection_test(self, saved_connection: MetrikaApiConnection) -> None:
        # in Metrica, you can't run constant queries like SELECT 1
        pass

    def test_make_connection(
            self, saved_connection: MetrikaApiConnection, conn_default_sync_us_manager: SyncUSManager
    ) -> None:
        conn = saved_connection
        usm = conn_default_sync_us_manager
        assert conn.uuid is not None
        assert conn.data.counter_creation_date

        usm_conn = usm.get_by_id(conn.uuid)
        assert conn.data.counter_creation_date == usm_conn.data.counter_creation_date
        revision_id_after_save = usm_conn.revision_id
        assert isinstance(revision_id_after_save, str)
        assert revision_id_after_save

        conn.data.name = '{} (changed)'.format(conn.data.name)
        usm.save(conn)
        revision_id_after_modify = conn.revision_id
        assert isinstance(revision_id_after_modify, str)
        assert revision_id_after_modify
        assert revision_id_after_modify != revision_id_after_save

        loaded_conn = usm.get_by_id(conn.uuid)
        assert loaded_conn.revision_id == revision_id_after_modify

    def check_data_source_templates(self, conn: MetrikaApiConnection, dsrc_templates: list[DataSourceTemplate]) -> None:
        expected_templates = sorted([
            DataSourceTemplate(
                title=ns.name,
                group=[],
                connection_id=conn.uuid,
                source_type=SOURCE_TYPE_METRICA_API,
                parameters={'db_name': ns.name},
            ) for ns in MetrikaApiCounterSource
        ], key=lambda el: el.title)
        assert expected_templates == sorted(dsrc_templates, key=lambda el: el.title)
