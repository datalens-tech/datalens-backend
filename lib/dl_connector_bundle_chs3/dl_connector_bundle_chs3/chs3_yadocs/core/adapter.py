import logging

from dl_connector_bundle_chs3.chs3_base.core.adapter import BaseAsyncFileS3Adapter
from dl_connector_bundle_chs3.chs3_yadocs.core.constants import CONNECTION_TYPE_DOCS


LOGGER = logging.getLogger(__name__)


class AsyncYaDocsFileS3Adapter(BaseAsyncFileS3Adapter):
    conn_type = CONNECTION_TYPE_DOCS
