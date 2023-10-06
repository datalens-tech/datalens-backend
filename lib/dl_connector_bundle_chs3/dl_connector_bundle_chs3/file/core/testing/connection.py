from __future__ import annotations

from typing import Any
import uuid

from dl_core.us_manager.us_manager_sync import SyncUSManager

from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE
from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection


def make_saved_file_connection(
    sync_usm: SyncUSManager,
    sources: list[FileS3Connection.FileDataSource],
    **kwargs: Any,
) -> FileS3Connection:
    conn_type = CONNECTION_TYPE_FILE

    conn_name = "{} test conn {}".format(conn_type.name, uuid.uuid4())
    conn = FileS3Connection.create_from_dict(
        data_dict=FileS3Connection.DataModel(
            sources=sources,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_FILE.name,
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
