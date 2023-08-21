import logging

from bi_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE
from bi_connector_bundle_chs3.chs3_base.core.adapter import BaseAsyncFileS3Adapter


LOGGER = logging.getLogger(__name__)


class AsyncFileS3Adapter(BaseAsyncFileS3Adapter):
    conn_type = CONNECTION_TYPE_FILE
