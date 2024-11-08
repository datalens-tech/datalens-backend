from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)
import uuid

from dl_constants.enums import (
    DataSourceCreatedVia,
    DataSourceRole,
    DataSourceType,
)
from dl_core.us_connection import get_connection_class
from dl_core.us_dataset import Dataset
from dl_core_testing.dataset_wrappers import EditableDatasetTestWrapper


if TYPE_CHECKING:
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase
    from dl_core.us_connection_base import ConnectionBase
    from dl_core.us_manager.us_manager_sync import SyncUSManager
    from dl_core_testing.database import (
        Db,
        DbTable,
    )


def make_ds_key(*args: str) -> str:
    return "/".join(["tests", *args])


def make_dataset(
    sync_usm: SyncUSManager,
    connection: Optional[ConnectionBase] = None,
    db_table: Optional[DbTable] = None,
    schema_name: Optional[str] = None,
    table_name: Optional[str] = None,
    created_from: Optional[DataSourceType] = None,
    db_name: Optional[str] = None,
    yt_path: Optional[str] = None,
    yt_cluster: Optional[str] = None,
    ds_info: Optional[dict] = None,
    dsrc_params: Optional[dict] = None,
    created_via: Optional[DataSourceCreatedVia] = None,
) -> Dataset:
    service_registry = sync_usm.get_services_registry()
    ds_info = dict(ds_info or {})
    db = db_table.db if db_table else None
    table_name = table_name or (db_table.name if db_table else None)
    schema_name = schema_name or (db_table.schema if db_table else None)
    db_name = db_name or (db.name if db else None)

    name = "Dataset {}".format(str(uuid.uuid4()))
    created_from = created_from or (get_created_from(db) if db is not None else None)
    dataset = Dataset.create_from_dict(
        Dataset.DataModel(name=name, **ds_info),
        ds_key=make_ds_key("datasets", name),
        us_manager=sync_usm,
    )
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=sync_usm)

    if created_via:
        ds_wrapper.set_created_via(created_via=created_via)

    if connection:
        dsrc_params = {
            **dict(
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
                path=yt_path,
                cluster=yt_cluster,
            ),
            **(dsrc_params or {}),
        }
        dsrc_params = {key: value for key, value in dsrc_params.items() if value is not None}
        source_id = str(uuid.uuid4())

        def conn_executor_factory() -> SyncConnExecutorBase:
            return service_registry.get_conn_executor_factory().get_sync_conn_executor(conn=connection)

        assert created_from is not None
        ds_wrapper.add_data_source(
            source_id=source_id,
            role=DataSourceRole.origin,
            connection_id=connection.uuid,
            created_from=created_from,
            parameters=dsrc_params,
        )
        sync_usm.load_dependencies(dataset)
        dsrc = ds_wrapper.get_data_source_strict(source_id=source_id, role=DataSourceRole.origin)
        ds_wrapper.update_data_source(
            source_id=source_id,
            role=DataSourceRole.origin,
            raw_schema=dsrc.get_schema_info(conn_executor_factory=conn_executor_factory).schema,
        )

        ds_wrapper.add_avatar(avatar_id=str(uuid.uuid4()), source_id=source_id, title="Main Avatar")

    return dataset


def get_created_from(db: Db) -> DataSourceType:
    conn_cls = get_connection_class(conn_type=db.conn_type)
    return conn_cls.source_type


def data_source_settings_from_table(table: DbTable, db_name: Optional[str] = None) -> dict:
    source_type = get_created_from(db=table.db)
    data: dict[str, Any] = {  # this still requires connection_id to be defined
        "source_type": source_type,
        "parameters": {
            "table_name": table.name,
            "db_name": db_name,
        },
    }

    if table.schema:
        data["parameters"]["schema_name"] = table.schema

    return data
