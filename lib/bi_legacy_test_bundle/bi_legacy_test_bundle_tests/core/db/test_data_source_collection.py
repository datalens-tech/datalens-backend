from __future__ import annotations

from typing import (
    Optional,
    Sequence,
)
import uuid

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE
from dl_constants.enums import (
    DataSourceRole,
    ManagedBy,
    UserDataType,
)
from dl_core.data_source.collection import DataSourceCollection
from dl_core.data_source_spec.collection import DataSourceCollectionSpec
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core.db.elements import SchemaColumn


def test_resolve_role(saved_ch_connection, default_sync_usm):
    us_manager = default_sync_usm
    default_raw_schema = (
        SchemaColumn(title="col1", name="name1", user_type=UserDataType.integer),
        SchemaColumn(title="col2", name="name2", user_type=UserDataType.integer),
    )
    outdated_raw_schema = default_raw_schema[:1]
    DEFAULT = "__default__"

    def _make_source_spec(
        db_name: Optional[str] = DEFAULT,
        table_name: Optional[str] = DEFAULT,
        raw_schema: Optional[Sequence[SchemaColumn]] = default_raw_schema,
    ) -> StandardSQLDataSourceSpec:
        return StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_CH_TABLE,
            connection_ref=saved_ch_connection.conn_ref,
            db_name=db_name if db_name is not DEFAULT else str(uuid.uuid4()),
            table_name=table_name if table_name is not DEFAULT else str(uuid.uuid4()),
            raw_schema=list(raw_schema) if raw_schema is not None else None,
        )

    us_manager.ensure_entry_preloaded(saved_ch_connection.conn_ref)
    us_entry_buffer = us_manager.get_entry_buffer()

    # direct, no sample
    dsrc_id = "1"
    mat_coll = DataSourceCollection(
        us_entry_buffer=us_entry_buffer,
        spec=DataSourceCollectionSpec(
            id=dsrc_id,
            managed_by=ManagedBy.user,
            origin=_make_source_spec(),
        ),
    )
    assert mat_coll.resolve_role() == DataSourceRole.origin
    assert mat_coll.resolve_role(for_preview=True) == DataSourceRole.origin

    mat_coll = DataSourceCollection(
        us_entry_buffer=us_entry_buffer,
        spec=DataSourceCollectionSpec(
            id=dsrc_id,
            managed_by=ManagedBy.user,
            origin=_make_source_spec(),
            sample=_make_source_spec(),
        ),
    )
    # direct, with sample
    assert mat_coll.resolve_role() == DataSourceRole.origin
    assert mat_coll.resolve_role(for_preview=True) == DataSourceRole.origin  # because sample has been disabled

    # direct, with outdated sample
    mat_coll = DataSourceCollection(
        us_entry_buffer=us_entry_buffer,
        spec=DataSourceCollectionSpec(
            id=dsrc_id,
            managed_by=ManagedBy.user,
            origin=_make_source_spec(),
            sample=_make_source_spec(raw_schema=outdated_raw_schema),
        ),
    )
    assert mat_coll.resolve_role() == DataSourceRole.origin
    assert mat_coll.resolve_role(for_preview=True) == DataSourceRole.origin
