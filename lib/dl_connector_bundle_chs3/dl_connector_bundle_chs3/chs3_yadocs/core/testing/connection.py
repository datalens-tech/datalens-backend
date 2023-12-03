from __future__ import annotations

from typing import Any
import uuid

from dl_core.us_manager.us_manager_sync import SyncUSManager

from dl_connector_bundle_chs3.chs3_yadocs.core.constants import CONNECTION_TYPE_DOCS
from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection


def make_saved_yadocs_connection(
    sync_usm: SyncUSManager,
    sources: list[YaDocsFileS3Connection.FileDataSource],
    **kwargs: Any,
) -> YaDocsFileS3Connection:
    conn_type = CONNECTION_TYPE_DOCS

    conn_name = "{} test conn {}".format(conn_type.name, uuid.uuid4())
    conn = YaDocsFileS3Connection.create_from_dict(
        data_dict=YaDocsFileS3Connection.DataModel(
            sources=sources,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_DOCS.name,
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
