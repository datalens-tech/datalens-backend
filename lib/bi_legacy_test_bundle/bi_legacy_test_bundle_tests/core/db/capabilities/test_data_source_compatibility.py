from __future__ import annotations

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from dl_core.dataset_capabilities import DatasetCapabilities
from dl_core_testing.connector import SOURCE_TYPE_TESTING
from dl_core_testing.dataset_wrappers import DatasetTestWrapper

from bi_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API


def test_source_can_be_added_to_clickhouse(
    saved_ch_dataset_per_func,
    saved_ch_connection,
    saved_testing_connection,
    default_sync_usm,
):
    dataset = saved_ch_dataset_per_func
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=default_sync_usm)
    capabilities = DatasetCapabilities(dataset=dataset, dsrc_coll_factory=ds_wrapper.dsrc_coll_factory)
    assert capabilities.source_can_be_added(connection_id=saved_ch_connection.uuid, created_from=SOURCE_TYPE_CH_TABLE)
    assert not capabilities.source_can_be_added(
        connection_id=saved_testing_connection.uuid, created_from=SOURCE_TYPE_TESTING
    )


def test_source_can_be_added_to_empty(
    saved_dataset_no_dsrc,
    saved_testing_connection,
    default_sync_usm,
):
    dataset = saved_dataset_no_dsrc
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=default_sync_usm)
    capabilities = DatasetCapabilities(dataset=dataset, dsrc_coll_factory=ds_wrapper.dsrc_coll_factory)
    assert capabilities.source_can_be_added(
        connection_id=saved_testing_connection.uuid, created_from=SOURCE_TYPE_TESTING
    )


def test_source_can_be_added_with_exclude_source_id(
    saved_ch_dataset,
    saved_ch_connection,
    saved_testing_connection,
    default_sync_usm,
):
    dataset = saved_ch_dataset
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=default_sync_usm)
    capabilities = DatasetCapabilities(dataset=dataset, dsrc_coll_factory=ds_wrapper.dsrc_coll_factory)
    ch_source_id = dataset.get_single_data_source_id()
    assert capabilities.source_can_be_added(
        connection_id=saved_ch_connection.uuid,
        created_from=SOURCE_TYPE_CH_TABLE,
        ignore_source_ids=[ch_source_id],
    )
    assert capabilities.source_can_be_added(
        connection_id=saved_testing_connection.uuid,
        created_from=SOURCE_TYPE_TESTING,
        ignore_source_ids=[ch_source_id],
    )


def test_replace_connection_types_single_clickhouse_conn(
    default_sync_usm,
    saved_ch_connection,
    saved_ch_dataset,
):
    connection = saved_ch_connection
    dataset = saved_ch_dataset
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=default_sync_usm)
    capabilities = DatasetCapabilities(dataset=dataset, dsrc_coll_factory=ds_wrapper.dsrc_coll_factory)
    default_sync_usm.load_dependencies(dataset)

    replacement_types = capabilities.get_compatible_connection_types(ignore_connection_ids=[connection.uuid])
    assert {
        CONNECTION_TYPE_METRICA_API,
        CONNECTION_TYPE_CLICKHOUSE,
        CONNECTION_TYPE_POSTGRES,
    }.issubset(replacement_types)
