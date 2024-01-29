from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
    Sequence,
    Type,
)
import uuid

from dl_constants.enums import ConnectionType
from dl_core.base_models import (
    ConnectionDataModelBase,
    EntryLocation,
    PathEntryLocation,
)
from dl_core.us_connection import get_connection_class
from dl_core.us_manager.us_manager import USManagerBase
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.database import Db


if TYPE_CHECKING:
    from dl_core.us_connection_base import ConnectionBase


def make_conn_key(*args) -> str:  # type: ignore  # TODO: fix
    return "/".join(["tests", *args])


def _ensure_entry_location(loc: Optional[EntryLocation], gen_path_postfix: Sequence[str]) -> EntryLocation:
    if loc is not None:
        return loc
    else:
        return PathEntryLocation(path=make_conn_key(*gen_path_postfix))


def make_connection_base(
    us_manager: USManagerBase,
    conn_type: ConnectionType,
    conn_name: str = None,  # type: ignore  # 2024-01-29 # TODO: Incompatible default for argument "conn_name" (default has type "None", argument has type "str")  [assignment]
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
        effective_data = data_model_cls(**data_dict)  # type: ignore
    else:
        raise TypeError(f"Unexpected data model class: {data_model_cls}")

    return conn_cls.create_from_dict(
        effective_data,
        ds_key=make_conn_key("connections", conn_name),
        type_=conn_type.name,
        meta={"title": conn_name, "state": "saved"},
        us_manager=us_manager,
    )


def make_connection(
    us_manager: USManagerBase,
    db: Optional[Db] = None,
    conn_type: Optional[ConnectionType] = None,
    conn_name: str = None,  # type: ignore  # 2024-01-29 # TODO: Incompatible default for argument "conn_name" (default has type "None", argument has type "str")  [assignment]
    data_dict: Optional[dict] = None,
) -> ConnectionBase:
    credentials: dict = {}
    if conn_type is None:
        assert db is not None
        conn_type = db.conn_type
        credentials = db.get_conn_credentials()
    assert conn_type is not None
    data_dict = {
        **credentials,
        **(data_dict or {}),
    }
    return make_connection_base(
        us_manager=us_manager,
        conn_type=conn_type,
        conn_name=conn_name,
        data_dict=data_dict,
    )


def make_saved_connection(  # type: ignore  # TODO: fix
    sync_usm: SyncUSManager,
    db: Optional[Db] = None,
    conn_type: Optional[ConnectionType] = None,
    conn_name: str = None,  # type: ignore  # 2024-01-29 # TODO: Incompatible default for argument "conn_name" (default has type "None", argument has type "str")  [assignment]
    data_dict: Optional[dict] = None,
) -> ConnectionBase:
    conn = make_connection(us_manager=sync_usm, db=db, conn_type=conn_type, conn_name=conn_name, data_dict=data_dict)
    sync_usm.save(conn)
    return conn
