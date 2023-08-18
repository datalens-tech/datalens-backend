import logging

from bi_constants.enums import ConnectionType

from bi_connector_bundle_chs3.chs3_base.core.adapter import BaseAsyncFileS3Adapter


LOGGER = logging.getLogger(__name__)


class AsyncGSheetsFileS3Adapter(BaseAsyncFileS3Adapter):
    conn_type = ConnectionType.gsheets_v2
