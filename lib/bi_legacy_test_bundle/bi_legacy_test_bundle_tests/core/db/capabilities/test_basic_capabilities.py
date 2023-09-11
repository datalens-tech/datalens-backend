from __future__ import annotations

from bi_constants.enums import DataSourceRole, JoinType

from bi_core.dataset_capabilities import DatasetCapabilities
from bi_core_testing.dataset import get_created_from
from bi_core_testing.dataset_wrappers import DatasetTestWrapper

from bi_connector_clickhouse.core.constants import (
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)


def test_compatibility_info(db, saved_connection, saved_dataset, default_sync_usm):
    dataset = saved_dataset
    connection = saved_connection
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=default_sync_usm)
    capabilities = DatasetCapabilities(dataset=dataset, dsrc_coll_factory=ds_wrapper.dsrc_coll_factory)

    dsrc = ds_wrapper.get_data_source_strict(
        source_id=dataset.get_single_data_source_id(), role=DataSourceRole.origin)
    dsrc_type = dsrc.spec.source_type
    actual_compat_stypes = capabilities.get_compatible_source_types()

    if dsrc_type == SOURCE_TYPE_CH_TABLE:
        assert actual_compat_stypes == frozenset([
            dsrc_type,
            SOURCE_TYPE_CH_SUBSELECT,
        ])
    else:
        assert actual_compat_stypes.issuperset(frozenset([dsrc_type]))

    assert capabilities.get_compatible_connection_types() == frozenset()

    dsrc = ds_wrapper.get_data_source_strict(source_id=dataset.get_single_data_source_id(), role=DataSourceRole.origin)
    dsrc_type = dsrc.spec.source_type
    compat_dsrc_types = capabilities.get_compatible_source_types()
    assert dsrc_type in compat_dsrc_types
    compat_conn_types = capabilities.get_compatible_connection_types()
    assert compat_conn_types == frozenset()

    assert capabilities.get_effective_connection_id() == connection.uuid
    assert capabilities.get_supported_join_types().issuperset({JoinType.inner, JoinType.left})
    assert capabilities.source_can_be_added(
        connection_id=connection.uuid,
        created_from=get_created_from(db=db)
    )
