from __future__ import annotations

import attr
import pytest

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE
from dl_connector_clickhouse.core.clickhouse.data_source import ClickHouseDataSource
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_postgresql.core.postgresql.constants import (
    CONNECTION_TYPE_POSTGRES,
    SOURCE_TYPE_PG_TABLE,
)
from dl_connector_postgresql.core.postgresql.data_source import PostgreSQLDataSource
from dl_constants.enums import (
    ConnectionType,
    UserDataType,
)
from dl_core import data_source
from dl_core.base_models import DefaultConnectionRef
from dl_core.data_source_spec.sql import (
    StandardSchemaSQLDataSourceSpec,
    StandardSQLDataSourceSpec,
)
from dl_core.db import SchemaColumn
from dl_core.db.native_type import (
    ClickHouseNativeType,
    CommonNativeType,
    GenericNativeType,
)
from dl_core.us_manager.storage_schemas.base import CtxKey
from dl_core.us_manager.storage_schemas.data_source_spec import GenericDataSourceSpecStorageSchema
from dl_core.us_manager.us_manager_sync import SyncUSManager


def common_col(
    name: str, ut: UserDataType, nt: str, conn_type: ConnectionType, nt_cls=CommonNativeType, **nt_kwargs
) -> SchemaColumn:
    return SchemaColumn(
        name=name,
        user_type=ut,
        native_type=nt_cls.normalize_name_and_create(
            name=nt,
            conn_type=conn_type,
            **nt_kwargs,
        ),
    )


def ch_col(
    name: str, ut: UserDataType, nt: str, conn_type=CONNECTION_TYPE_CLICKHOUSE, nt_cls=ClickHouseNativeType, **nt_kwargs
) -> SchemaColumn:
    return common_col(name=name, ut=ut, nt=nt, conn_type=conn_type, nt_cls=nt_cls, **nt_kwargs)


def rs_for(ct: ConnectionType, *rs: SchemaColumn):
    # noinspection PyProtectedMember
    return [sc._replace(native_type=attr.evolve(sc.native_type, conn_type=ct)) for sc in rs]


# TODO FIX: Add more data sources and currently used dicts from US
_DS_FACTORY = {
    "ch_user_1": lambda usm: ClickHouseDataSource(
        us_entry_buffer=usm.get_entry_buffer(),
        spec=StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_CH_TABLE,
            connection_ref=DefaultConnectionRef(conn_id="12xl1k123"),
            db_version="1.1.2",
            db_name="some_db",
            table_name="some_user_table",
            data_dump_id=None,
            raw_schema=rs_for(CONNECTION_TYPE_CLICKHOUSE, ch_col("pk", UserDataType.integer, "uint64")),
        ),
    ),
    "ch_postgres": lambda usm: PostgreSQLDataSource(
        us_entry_buffer=usm.get_entry_buffer(),
        spec=StandardSchemaSQLDataSourceSpec(
            source_type=SOURCE_TYPE_PG_TABLE,
            connection_ref=DefaultConnectionRef(conn_id="12xl1k123"),
            db_version="1.1.2",
            schema_name="some_schema",
            db_name="some_db",
            table_name="some_user_table",
            data_dump_id=None,
            raw_schema=rs_for(
                CONNECTION_TYPE_POSTGRES,
                common_col("pk", UserDataType.integer, "bigint", conn_type=CONNECTION_TYPE_POSTGRES),
            ),
        ),
    ),
}


@pytest.fixture(scope="function", params=_DS_FACTORY.keys())
def supported_data_source(sync_usm: SyncUSManager, request):
    p = request.param
    return _DS_FACTORY[p](sync_usm)


def test_data_source_spec_round_serialization(supported_data_source: data_source.DataSource, sync_usm):
    dsrc = supported_data_source

    ser_ctx = {CtxKey.us_manager: sync_usm}
    schema = GenericDataSourceSpecStorageSchema(context=ser_ctx)

    dumped_dsrc_spec = schema.dump(dsrc.spec)
    loaded_dsrc_spec = schema.load(dumped_dsrc_spec)

    if type(loaded_dsrc_spec.raw_schema[0].native_type) is GenericNativeType:  # transition-case
        for some_ds in (dsrc.spec, loaded_dsrc_spec):
            some_ds.raw_schema = [sc._replace(native_type=sc.native_type.as_generic) for sc in some_ds.raw_schema]

    assert dsrc.spec.raw_schema == loaded_dsrc_spec.raw_schema
    assert dsrc.spec == loaded_dsrc_spec
