from __future__ import annotations

from typing import Any
import uuid

from dl_core.us_manager.us_manager_sync import SyncUSManager

from bi_connector_metrica.core.constants import (
    CONNECTION_TYPE_APPMETRICA_API,
    CONNECTION_TYPE_METRICA_API,
)
from bi_connector_metrica.core.us_connection import (
    AppMetricaApiConnection,
    MetrikaApiConnection,
)


def make_saved_metrika_api_connection(
    sync_usm: SyncUSManager,
    counter_id: str,
    token: str,
    **kwargs: Any,
) -> MetrikaApiConnection:
    conn_name = "metrica api test_revision_id conn %s" % uuid.uuid4()
    conn = MetrikaApiConnection.create_from_dict(
        MetrikaApiConnection.DataModel(
            token=token,
            counter_id=counter_id,
            name=conn_name,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_METRICA_API.name,
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn


def make_saved_appmetrica_api_connection(
    sync_usm: SyncUSManager, counter_id: str, token: str, **kwargs: Any
) -> AppMetricaApiConnection:
    conn_name = "appmetrica api test_revision_id conn %s" % uuid.uuid4()
    conn = AppMetricaApiConnection.create_from_dict(
        AppMetricaApiConnection.DataModel(
            token=token,
            counter_id=counter_id,
            name=conn_name,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_APPMETRICA_API.name,
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
