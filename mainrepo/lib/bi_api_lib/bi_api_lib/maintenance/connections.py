from __future__ import annotations

import logging
import uuid

from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.constants import (
    CONNECTION_TYPE_CH_GEO_FILTERED,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.us_connection import ConnectionClickhouseGeoFiltered
from bi_core.base_models import PathEntryLocation
from bi_core.maintenance.logging_config import configure_logging_for_shell
from bi_core.us_manager.us_manager_sync import SyncUSManager


LOGGER = logging.getLogger(__name__)


def evolve_ch_conn_to_geo_filtered(
        usm: SyncUSManager,
        src_conn_id: str,
        mp_product_id: str,
        allowed_tables: list[str],
        db_name: str,
) -> None:
    configure_logging_for_shell()

    src_conn = usm.get_by_id(src_conn_id)
    conn_loc = src_conn.entry_key
    assert isinstance(conn_loc, PathEntryLocation)
    new_path = conn_loc.path + ' geolayer conn ' + str(uuid.uuid4())

    data_dict = dict(
        mp_product_id=mp_product_id,
        allowed_tables=allowed_tables,
        host=src_conn.data.host,
        port=src_conn.data.port,
        username=src_conn.data.username,
        password=src_conn.password,  # type: ignore  # TODO: fix
        db_name=db_name,
    )

    conn = ConnectionClickhouseGeoFiltered.create_from_dict(
        data_dict,
        ds_key=PathEntryLocation(new_path),
        type_=CONNECTION_TYPE_CH_GEO_FILTERED.name,
        meta={},
        us_manager=usm,
    )
    usm.save(conn)
    LOGGER.info(f'Connection created. id: {conn.uuid}, path: {new_path}')
