from __future__ import annotations

import uuid
from typing import Optional

from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.constants import (
    CONNECTION_TYPE_CH_GEO_FILTERED,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.us_connection import ConnectionClickhouseGeoFiltered
from bi_core_testing.connection import make_conn_key


def make_saved_ch_geo_filtered_connection(
        sync_usm: SyncUSManager,
        host: str,
        port: int,
        username: str,
        password: str,
        db_name: str,
        mp_product_id: str = 'mp_product_test_id',
        allowed_tables: Optional[list[str]] = None,
        data_export_forbidden: bool = True,
) -> ConnectionClickhouseGeoFiltered:
    conn_name = 'ch_geo_filtered test conn {}'.format(uuid.uuid4())
    if allowed_tables is None:
        allowed_tables = ['geolayer_table_1', 'geolayer_table_2']
    conn = ConnectionClickhouseGeoFiltered.create_from_dict(
        ConnectionClickhouseGeoFiltered.DataModel(
            host=host,
            port=port,
            username=username,
            password=password,
            db_name=db_name,
            mp_product_id=mp_product_id,
            allowed_tables=allowed_tables,
            data_export_forbidden=data_export_forbidden,
        ),
        ds_key=make_conn_key('connections', conn_name),
        type_=CONNECTION_TYPE_CH_GEO_FILTERED.name,
        meta={'title': conn_name, 'state': 'saved'},
        us_manager=sync_usm,
    )
    sync_usm.save(conn)
    return conn
