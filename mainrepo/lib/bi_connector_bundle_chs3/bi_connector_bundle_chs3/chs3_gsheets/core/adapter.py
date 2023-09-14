import logging

from bi_connector_bundle_chs3.chs3_base.core.adapter import BaseAsyncFileS3Adapter
from bi_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2

LOGGER = logging.getLogger(__name__)


class AsyncGSheetsFileS3Adapter(BaseAsyncFileS3Adapter):
    conn_type = CONNECTION_TYPE_GSHEETS_V2
