import logging

from dl_connector_bundle_chs3.chs3_base.core.adapter import BaseAsyncFileS3Adapter
from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE


LOGGER = logging.getLogger(__name__)


class AsyncFileS3Adapter(BaseAsyncFileS3Adapter):
    conn_type = CONNECTION_TYPE_FILE
