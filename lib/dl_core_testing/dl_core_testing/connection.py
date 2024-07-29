from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
    Type,
)
import uuid

from dl_constants.enums import ConnectionType
from dl_core.base_models import ConnectionDataModelBase
from dl_core.us_connection import get_connection_class
from dl_core.us_manager.us_manager import USManagerBase
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.database import Db


if TYPE_CHECKING:
    from dl_core.us_connection_base import ConnectionBase


def make_conn_key(*args: str) -> str:
    return "/".join(["tests", *args])


def make_connection(
    us_manager: USManagerBase,
    conn_type: ConnectionType,
    conn_name: Optional[str] = None,
    data_dict: Optional[dict] = None,
    conn_cls: Optional[Type[ConnectionBase]] = None,
) -> ConnectionBase:
    conn_name = conn_name or "{} test conn {}".format(conn_type.name, uuid.uuid4())
    data_dict = data_dict or {}
    if conn_cls is None:
        conn_cls = get_connection_class(conn_type)

    effective_data: dict | ConnectionDataModelBase
    data_model_cls = conn_cls.DataModel

    if issubclass(data_model_cls, ConnectionDataModelBase):
        effective_data = data_model_cls(**data_dict)
    else:
        raise TypeError(f"Unexpected data model class: {data_model_cls}")

    return conn_cls.create_from_dict(
        effective_data,
        ds_key=make_conn_key("connections", conn_name),
        type_=conn_type.name,
        meta={"title": conn_name, "state": "saved"},
        us_manager=us_manager,
    )


def make_connection_from_db(
    us_manager: USManagerBase,
    db: Db,
    conn_name: Optional[str] = None,
    data_dict: Optional[dict] = None,
) -> ConnectionBase:
    conn_type = db.conn_type
    credentials = db.get_conn_credentials()
    data_dict = {
        **credentials,
        **(data_dict or {}),
    }
    return make_connection(
        us_manager=us_manager,
        conn_type=conn_type,
        conn_name=conn_name,
        data_dict=data_dict,
    )


def make_saved_connection(
    sync_usm: SyncUSManager,
    conn_type: ConnectionType,
    conn_name: Optional[str] = None,
    data_dict: Optional[dict] = None,
) -> ConnectionBase:
    conn = make_connection(us_manager=sync_usm, conn_type=conn_type, conn_name=conn_name, data_dict=data_dict)
    sync_usm.save(conn)
    return conn


def make_saved_connection_from_db(
    sync_usm: SyncUSManager,
    db: Db,
    conn_name: Optional[str] = None,
    data_dict: Optional[dict] = None,
) -> ConnectionBase:
    conn = make_connection_from_db(us_manager=sync_usm, db=db, conn_name=conn_name, data_dict=data_dict)
    sync_usm.save(conn)
    return conn
