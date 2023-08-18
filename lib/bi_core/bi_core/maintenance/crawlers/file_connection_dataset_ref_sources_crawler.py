import logging
from typing import Any, AsyncIterable, Optional, Type, TYPE_CHECKING

import attr

from bi_constants.enums import ConnectionType, DataSourceCollectionType, ManagedBy, CreateDSFrom

from bi_core import exc
from bi_core.base_models import DefaultConnectionRef
from bi_core.data_source_spec.collection import DataSourceCollectionSpec, DataSourceCollectionProxySpec
from bi_core.maintenance.us_crawler_base import USEntryCrawler
from bi_core.us_dataset import Dataset
from bi_core.us_manager.us_manager_async import AsyncUSManager

if TYPE_CHECKING:
    from bi_core.data_source_spec.collection import DataSourceCollectionSpecBase  # noqa


LOGGER = logging.getLogger(__name__)


@attr.s
class RefToNonRefDatasetSourceMigrationCrawler(USEntryCrawler):
    ENTRY_TYPE = Dataset

    def get_raw_entry_iterator(self, crawl_all_tenants: bool = True) -> AsyncIterable[dict[str, Any]]:
        return self.usm.get_raw_collection(
            entry_scope='dataset',
            entry_type=None,
            all_tenants=crawl_all_tenants,
        )

    async def process_entry_get_save_flag(  # type: ignore
            self,
            entry: Dataset,
            logging_extra: dict[str, Any],
            usm: Optional[AsyncUSManager] = None,
    ) -> tuple[bool, str]:

        # Connector module is not available to import tests
        from bi_connector_bundle_chs3.chs3_base.core.data_source_spec import BaseFileS3DataSourceSpec  # type: ignore
        from bi_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection  # type: ignore
        from bi_connector_bundle_chs3.file.core.data_source_spec import FileS3DataSourceSpec  # type: ignore
        from bi_connector_bundle_chs3.chs3_gsheets.core.data_source_spec import GSheetsFileS3DataSourceSpec  # type: ignore

        assert usm is not None

        source_collections_to_update: dict[int, DataSourceCollectionSpec] = {}

        for idx, dsrc_coll_config in enumerate(entry.data.source_collections):  # type: int, DataSourceCollectionSpecBase
            if dsrc_coll_config.managed_by == ManagedBy.user:
                if dsrc_coll_config.dsrc_coll_type == DataSourceCollectionType.collection:
                    continue
                assert dsrc_coll_config.dsrc_coll_type == DataSourceCollectionType.connection_ref
                assert isinstance(dsrc_coll_config, DataSourceCollectionProxySpec)

                connection_ref: Optional[DefaultConnectionRef] = getattr(dsrc_coll_config, 'connection_ref', None)
                if connection_ref is None:
                    continue

                try:
                    connection: BaseFileS3Connection = await usm.get_by_id(connection_ref.conn_id, BaseFileS3Connection)
                except exc.USObjectNotFoundException:
                    LOGGER.info(f'Connection {connection_ref.conn_id} is not found - skipping')
                    return False, 'Connection no found'

                if connection.conn_type not in (ConnectionType.file, ConnectionType.gsheets_v2):
                    return False, f'Skip {connection.type_}-based dataset'

                try:
                    origin_src = connection.get_file_source_by_id(dsrc_coll_config.source_id)
                    raw_schema = origin_src.raw_schema
                except exc.SourceDoesNotExist:
                    LOGGER.info(
                        f'Can not find origin source {dsrc_coll_config.source_id}'
                        f' in the connection {connection_ref.conn_id} for the dataset {entry.uuid}'
                        f' - going to set empty raw_schema in the dataset',
                        extra=logging_extra
                    )
                    raw_schema = []

                dsrc_spec_cls: Type[BaseFileS3DataSourceSpec] = {
                    ConnectionType.file: FileS3DataSourceSpec,
                    ConnectionType.gsheets_v2: GSheetsFileS3DataSourceSpec,
                }[connection.conn_type]

                source_type: CreateDSFrom = {
                    ConnectionType.file: CreateDSFrom.FILE_S3_TABLE,
                    ConnectionType.gsheets_v2: CreateDSFrom.GSHEETS_V2,
                }[connection.conn_type]

                new_dsrc_coll = DataSourceCollectionSpec(
                    id=dsrc_coll_config.id,
                    title=dsrc_coll_config.title,
                    managed_by=dsrc_coll_config.managed_by,
                    valid=True,
                    origin=dsrc_spec_cls(
                        source_type=source_type,
                        connection_ref=DefaultConnectionRef(conn_id=connection_ref.conn_id),
                        raw_schema=raw_schema,
                        origin_source_id=dsrc_coll_config.source_id,
                    ),
                    materialization=None,
                    sample=None,
                )

                source_collections_to_update[idx] = new_dsrc_coll

        if not source_collections_to_update:
            return False, 'Nothing to do for entry'

        for idx, new_dsrc_coll in source_collections_to_update.items():
            entry.data.source_collections[idx] = new_dsrc_coll

        return True, ''
