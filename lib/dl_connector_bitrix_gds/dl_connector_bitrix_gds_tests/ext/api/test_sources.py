import abc

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.data_api_base import DataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase

from dl_connector_bitrix_gds.core.constants import SOURCE_TYPE_BITRIX_GDS
from dl_connector_bitrix_gds_tests.ext.api.base import (
    BitrixDatasetTestBase,
    BitrixSmartTablesDatasetTestBase,
)
from dl_connector_bitrix_gds_tests.ext.config import DB_NAME


class BitrixSourcesTestBase(DatasetTestBase, DataApiTestBase, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def do_check_source(self, source: dict) -> bool:
        """Should return true if and only if we want to test this source in this class"""
        pass

    def test_sources(
        self,
        control_api_sync_client: SyncHttpClientBase,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        data_api: SyncHttpDataApiV2,
    ):
        conn_id = saved_connection_id

        sources_resp = control_api_sync_client.get(f"/api/v1/connections/{conn_id}/info/sources")
        assert sources_resp.status_code == 200, sources_resp.json

        sources_checked = 0
        for source in sources_resp.json["sources"]:
            if not self.do_check_source(source):
                continue

            dataset_params = dict(
                source_type=SOURCE_TYPE_BITRIX_GDS.name,
                title=source["title"],
                parameters=dict(
                    db_name=DB_NAME,
                    table_name=source["title"],
                ),
            )
            ds = self.make_basic_dataset(
                control_api=control_api,
                connection_id=saved_connection_id,
                dataset_params=dataset_params,
            )
            preview_resp = data_api.get_preview(dataset=ds)
            assert preview_resp.status_code == 200, preview_resp.response_errors

            sources_checked += 1

        assert sources_checked > 1


class TestBitrixSources(BitrixDatasetTestBase, BitrixSourcesTestBase):
    def do_check_source(self, source: dict) -> bool:
        title = source["title"]
        return title not in [
            "telephony_call",
            "crm_lead_uf",
            "crm_deal_uf",
            "crm_company_uf",
            "crm_contact_uf",
        ] and not title.startswith("crm_dynamic_items_")


class TestBitrixSmartTablesSources(BitrixSmartTablesDatasetTestBase, BitrixSourcesTestBase):
    def do_check_source(self, source: dict) -> bool:
        return source["title"].startswith("crm_dynamic_items_")
