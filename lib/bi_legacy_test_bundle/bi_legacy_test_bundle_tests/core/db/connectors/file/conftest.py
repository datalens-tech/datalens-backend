from __future__ import annotations

import uuid

import pytest

from dl_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE
from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_constants.enums import FileProcessingStatus
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.connection import make_conn_key


@pytest.fixture(scope="function")
def saved_file_connection(default_sync_usm: SyncUSManager):
    us_manager = default_sync_usm
    conn_name = "file test conn {}".format(uuid.uuid4())
    data = FileS3Connection.DataModel(
        sources=[
            FileS3Connection.FileDataSource(
                id=f"source_{_}",
                file_id=str(uuid.uuid4()),
                title="Source 1",
                s3_filename=str(uuid.uuid4()),
                raw_schema=[],
                status=FileProcessingStatus.ready,
            )
            for _ in range(2)
        ]
    )
    conn = FileS3Connection.create_from_dict(
        data,
        ds_key=make_conn_key("connections", conn_name),
        type_=CONNECTION_TYPE_FILE.name,
        meta={"title": conn_name, "state": "saved"},
        us_manager=us_manager,
    )
    us_manager.save(conn)
    yield conn
    us_manager.delete(conn)
