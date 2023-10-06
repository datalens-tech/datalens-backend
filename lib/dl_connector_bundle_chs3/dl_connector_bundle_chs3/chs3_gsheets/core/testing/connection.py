from __future__ import annotations

from typing import Any
import uuid

from dl_core.us_manager.us_manager_sync import SyncUSManager

from dl_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2
from dl_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection


def make_saved_gsheets_v2_connection(
    sync_usm: SyncUSManager,
    sources: list[GSheetsFileS3Connection.FileDataSource],
    **kwargs: Any,
) -> GSheetsFileS3Connection:
    conn_type = CONNECTION_TYPE_GSHEETS_V2

    conn_name = "{} test conn {}".format(conn_type.name, uuid.uuid4())
    conn = GSheetsFileS3Connection.create_from_dict(
        data_dict=GSheetsFileS3Connection.DataModel(
            sources=sources,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_GSHEETS_V2.name,
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
