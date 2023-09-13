from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Optional, Any

from bi_constants.enums import CreateDSFrom, DataSourceRole, DataSourceCreatedVia

from bi_core.us_dataset import Dataset
from bi_core.us_connection_base import ExecutorBasedMixin
from bi_core_testing.dataset_wrappers import EditableDatasetTestWrapper
from bi_core.us_connection import get_connection_class

if TYPE_CHECKING:
    from bi_core_testing.database import Db, DbTable
    from bi_core.us_connection_base import ConnectionBase
    from bi_core.us_manager.us_manager_sync import SyncUSManager
    from bi_core.connection_executors.sync_base import SyncConnExecutorBase


def make_ds_key(*args) -> str:  # type: ignore  # TODO: fix
    return '/'.join(['tests', *args])


def make_dataset(  # type: ignore  # TODO: fix
        sync_usm: SyncUSManager,
        connection: Optional[ConnectionBase] = None,
        db_table: Optional[DbTable] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        created_from: Optional[CreateDSFrom] = None,
        db_name: Optional[str] = None,
        yt_path: Optional[str] = None,
        yt_cluster: Optional[str] = None,
        ds_info: Optional[dict] = None,
        dsrc_params: Optional[dict] = None,
        created_via: Optional[DataSourceCreatedVia] = None,
):
    service_registry = sync_usm.get_services_registry()
    ds_info = dict(ds_info or {})
    db = db_table.db if db_table else None
    table_name = table_name or (db_table.name if db_table else None)
    schema_name = schema_name or (db_table.schema if db_table else None)
    db_name = db_name or (db.name if db else None)

    name = 'Dataset {}'.format(str(uuid.uuid4()))
    created_from = created_from or (get_created_from(db) if db is not None else None)
    dataset = Dataset.create_from_dict(
        Dataset.DataModel(
            name=name,
            **ds_info
        ),
        ds_key=make_ds_key('datasets', name),
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
            assert isinstance(connection, ExecutorBasedMixin)
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

        ds_wrapper.add_avatar(avatar_id=str(uuid.uuid4()), source_id=source_id, title='Main Avatar')

    return dataset


def get_created_from(db: Db) -> CreateDSFrom:
    conn_cls = get_connection_class(conn_type=db.conn_type)
    source_type = conn_cls.source_type
    assert source_type is not None
    return source_type


def data_source_settings_from_table(table: DbTable) -> dict:
    source_type = get_created_from(db=table.db)
    data: dict[str, Any] = {  # this still requires connection_id to be defined
        'source_type': source_type,
        'parameters': {
            'table_name': table.name,
            'db_name': table.db.name if source_type == CreateDSFrom.CH_TABLE else None,  # FIXME
        },
    }

    if table.schema:
        data['parameters']['schema_name'] = table.schema

    return data
