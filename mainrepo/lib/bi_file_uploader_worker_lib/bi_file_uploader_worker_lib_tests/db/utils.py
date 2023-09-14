import uuid

from bi_constants.enums import (
    DataSourceRole,
    FileProcessingStatus,
)
from bi_core.us_manager.us_manager_async import AsyncUSManager
from bi_core_testing.connection import make_conn_key

from bi_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE
from bi_connector_bundle_chs3.file.core.us_connection import FileS3Connection


async def create_file_connection(us_manager: AsyncUSManager, file_id, source_id, raw_schema, src_title="Source 1"):
    conn_name = "file test conn {}".format(uuid.uuid4())
    data = FileS3Connection.DataModel(
        sources=[
            FileS3Connection.FileDataSource(
                id=source_id,
                file_id=file_id,
                title=src_title,
                status=FileProcessingStatus.in_progress,
            )
        ]
    )
    conn = FileS3Connection.create_from_dict(
        data,
        ds_key=make_conn_key("connections", conn_name),
        type_=CONNECTION_TYPE_FILE.name,
        meta={"title": conn_name, "state": "saved"},
        us_manager=us_manager,
    )
    conn.update_data_source(
        source_id,
        role=DataSourceRole.origin,
        raw_schema=raw_schema,
    )
    await us_manager.save(conn)
    return conn
