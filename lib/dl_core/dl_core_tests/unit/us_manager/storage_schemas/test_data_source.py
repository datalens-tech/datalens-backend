from __future__ import annotations

import pytest

from dl_constants.enums import UserDataType
from dl_core import data_source
from dl_core.base_models import DefaultConnectionRef
from dl_core.data_source_spec.sql import (
    StandardSchemaSQLDataSourceSpec,
    StandardSQLDataSourceSpec,
)
from dl_core.db import SchemaColumn
from dl_core.us_manager.storage_schemas.base import CtxKey
from dl_core.us_manager.storage_schemas.data_source_spec import GenericDataSourceSpecStorageSchema
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core.us_manager.us_manager_sync_mock import MockedSyncUSManager
from dl_type_transformer.native_type import (
    ClickHouseNativeType,
    CommonNativeType,
    GenericNativeType,
)

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE
from dl_connector_clickhouse.core.clickhouse.data_source import ClickHouseDataSource
from dl_connector_postgresql.core.postgresql.constants import SOURCE_TYPE_PG_TABLE
from dl_connector_postgresql.core.postgresql.data_source import PostgreSQLDataSource


def common_col(
    name: str,
    ut: UserDataType,
    nt: str,
    nt_cls: type = CommonNativeType,
    **nt_kwargs: object,
) -> SchemaColumn:
    return SchemaColumn(
        name=name,
        user_type=ut,
        native_type=nt_cls.normalize_name_and_create(name=nt, **nt_kwargs),
    )


def ch_col(
    name: str,
    ut: UserDataType,
    nt: str,
    nt_cls: type = ClickHouseNativeType,
    **nt_kwargs: object,
) -> SchemaColumn:
    return common_col(name=name, ut=ut, nt=nt, nt_cls=nt_cls, **nt_kwargs)


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
            raw_schema=[ch_col("pk", UserDataType.integer, "uint64")],
        ),
        dataset_parameter_values={},
        dataset_template_enabled=False,
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
            raw_schema=[common_col("pk", UserDataType.integer, "bigint")],
        ),
        dataset_parameter_values={},
        dataset_template_enabled=False,
    ),
}


@pytest.fixture
def sync_usm(bi_context, crypto_keys_config, default_service_registry) -> SyncUSManager:
    return MockedSyncUSManager(
        bi_context=bi_context,
        crypto_keys_config=crypto_keys_config,
        services_registry=default_service_registry,
    )


@pytest.fixture(params=_DS_FACTORY.keys())
def supported_data_source(sync_usm: SyncUSManager, request: pytest.FixtureRequest) -> data_source.DataSource:
    p = request.param
    return _DS_FACTORY[p](sync_usm)


def test_data_source_spec_round_serialization(
    supported_data_source: data_source.DataSource,
    sync_usm: SyncUSManager,
) -> None:
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
