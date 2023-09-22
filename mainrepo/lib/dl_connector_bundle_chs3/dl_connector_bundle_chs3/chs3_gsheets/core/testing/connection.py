from __future__ import annotations

import asyncio
import uuid

from dl_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2
from dl_connector_clickhouse.db_testing.engine_wrapper import ClickhouseDbEngineConfig
from dl_constants.enums import FileProcessingStatus
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.database import DbTable


def make_saved_gsheets_v2_connection(  # type: ignore  # TODO: fix
    sync_usm: SyncUSManager, clickhouse_table: DbTable, filename: str, **kwargs
):
    from dl_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection
    from dl_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions
    from dl_connector_clickhouse.core.clickhouse_base.connection_executors import ClickHouseSyncAdapterConnExecutor
    from dl_connector_clickhouse.core.clickhouse_base.dto import ClickHouseConnDTO
    from dl_core.connection_executors import (
        ExecutionMode,
        SyncWrapperForAsyncConnExecutor,
    )
    from dl_core.connection_models import TableIdent
    from dl_core.connections_security.base import InsecureConnectionSecurityManager

    conn_name = "gsheets_v2 conn %s" % uuid.uuid4()
    engine_config = clickhouse_table.db.engine_config
    assert isinstance(engine_config, ClickhouseDbEngineConfig)
    cluster = engine_config.cluster
    assert cluster is not None
    with SyncWrapperForAsyncConnExecutor(
        async_conn_executor=ClickHouseSyncAdapterConnExecutor(  # type: ignore  # TODO: fix
            conn_dto=ClickHouseConnDTO(
                conn_id=None,
                protocol="http",
                endpoint=None,
                cluster_name=cluster,
                multihosts=clickhouse_table.db.get_conn_hosts(),  # type: ignore  # TODO: fix
                **clickhouse_table.db.get_conn_credentials(full=True),
            ),
            conn_options=CHConnectOptions(max_execution_time=None, connect_timeout=None, total_timeout=None),
            req_ctx_info=None,
            exec_mode=ExecutionMode.DIRECT,
            sec_mgr=InsecureConnectionSecurityManager(),
            remote_qe_data=None,
            tpe=None,
            conn_hosts_pool=clickhouse_table.db.get_conn_hosts(),
            host_fail_callback=lambda h: None,
        ),
        loop=asyncio.get_event_loop(),
    ) as ce:
        raw_schema = ce.get_table_schema_info(
            TableIdent(
                db_name=clickhouse_table.db.name,
                schema_name=None,
                table_name=clickhouse_table.name,
            )
        ).schema
    data_dict = GSheetsFileS3Connection.DataModel(
        sources=[
            GSheetsFileS3Connection.FileDataSource(
                id=str(uuid.uuid4()),
                file_id=str(uuid.uuid4()),
                title=f"Title -- {filename.upper()}",
                s3_filename=filename,
                raw_schema=raw_schema,
                status=FileProcessingStatus.ready,
                sheet_id=0,
                first_line_is_header=True,
                spreadsheet_id="some_spreadsheet_id",
            ),
        ],
    )
    conn = GSheetsFileS3Connection.create_from_dict(
        data_dict, ds_key=conn_name, type_=CONNECTION_TYPE_GSHEETS_V2.name, us_manager=sync_usm, **kwargs
    )
    sync_usm.save(conn)
    return conn
