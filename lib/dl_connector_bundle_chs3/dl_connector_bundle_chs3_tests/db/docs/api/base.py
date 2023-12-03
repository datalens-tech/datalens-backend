import datetime
import logging

from aiohttp import web
import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_core.us_manager.us_manager_async import AsyncUSManager

from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.api.base import CHS3ConnectionApiTestBase
from dl_connector_bundle_chs3_tests.db.base.api.data import CHS3DataApiTestBase
from dl_connector_bundle_chs3_tests.db.base.api.dataset import CHS3DatasetTestBase
from dl_connector_bundle_chs3_tests.db.docs.core.base import BaseYaDocsFileS3TestClass


LOGGER = logging.getLogger(__name__)


class YaDocsFileS3ApiConnectionTestBase(
    BaseYaDocsFileS3TestClass,
    CHS3ConnectionApiTestBase[YaDocsFileS3Connection],
):
    @pytest.fixture(scope="function")
    async def mock_file_uploader_api(
        self,
        aiohttp_server,
        bi_test_config: ApiTestEnvironmentConfiguration,
        async_us_manager: AsyncUSManager,
    ) -> None:
        async def mocked_update_connection_data_internal(request: web.Request) -> web.Response:
            req_data = await request.json()
            conn_id: str = req_data["connection_id"]
            sources_to_update = [src["id"] for src in req_data["sources"]]

            conn = await async_us_manager.get_by_id(conn_id, YaDocsFileS3Connection)
            for src_id in sources_to_update:
                src: YaDocsFileS3Connection.FileDataSource = conn.get_file_source_by_id(src_id)
                src.data_updated_at = datetime.datetime.now(datetime.timezone.utc)
                LOGGER.info(f"Successfully updated source id {src_id}")
            await async_us_manager.save(conn)

            return web.HTTPOk()

        app = web.Application()
        app.router.add_route("POST", "/api/v2/update_connection_data_internal", mocked_update_connection_data_internal)

        server = await aiohttp_server(app, port=bi_test_config.file_uploader_api_port)

        yield

        await server.close()

    @pytest.fixture(scope="function")
    def connection_params(
        self,
        sample_file_data_source: YaDocsFileS3Connection.FileDataSource,
    ) -> dict:
        return dict(
            refresh_enabled=True,
            sources=[
                dict(
                    file_id=sample_file_data_source.file_id,
                    id=sample_file_data_source.id,
                    title=sample_file_data_source.title,
                ),
            ],
        )


class YaDocsFileS3DatasetTestBase(YaDocsFileS3ApiConnectionTestBase, CHS3DatasetTestBase[YaDocsFileS3Connection]):
    pass


class YaDocsFileS3DataApiTestBase(YaDocsFileS3DatasetTestBase, CHS3DataApiTestBase[YaDocsFileS3Connection]):
    pass
