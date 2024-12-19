from copy import deepcopy

from dl_constants.enums import (
    DataSourceType,
    RawSQLLevel,
)
from dl_core_testing.dataset import add_dataset_source
from dl_core_tests.db.base import DefaultCoreTestClass

from dl_connector_clickhouse.core.clickhouse.constants import (
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)


FAKE_CREATED_FROM = DataSourceType.declare("FAKE_SOURCE")


class TestDataSource(DefaultCoreTestClass):
    raw_sql_level = RawSQLLevel.subselect

    def test_find_data_source_configuration(
        self,
        saved_connection,
        saved_dataset,
        sync_us_manager,
        editable_dataset_wrapper,
        dsrc_params,
    ):
        # table source
        params = deepcopy(dsrc_params)
        assert (
            saved_dataset.find_data_source_configuration(
                connection_id=saved_connection.uuid,
                created_from=SOURCE_TYPE_CH_TABLE,
                parameters=params,
            )
            is not None
        )

        # wrong created_from
        assert (
            saved_dataset.find_data_source_configuration(
                connection_id=saved_connection.uuid,
                created_from=FAKE_CREATED_FROM,
                parameters=params,
            )
            is None
        )

        # wrong table name
        wrong_params = params | dict(table_name="fake_table_name")
        assert (
            saved_dataset.find_data_source_configuration(
                connection_id=saved_connection.uuid,
                created_from=SOURCE_TYPE_CH_TABLE,
                parameters=wrong_params,
            )
            is None
        )

        # create a subsql source with title
        subsql_params = dict(subsql="SELECT 1 AS A")
        title = "My SQL"
        add_dataset_source(
            sync_usm=sync_us_manager,
            connection=saved_connection,
            dataset=saved_dataset,
            editable_dataset_wrapper=editable_dataset_wrapper,
            created_from=SOURCE_TYPE_CH_SUBSELECT,
            dsrc_params=subsql_params,
            title=title,
        )
        sync_us_manager.save(saved_dataset)
        source_id = saved_dataset.find_data_source_configuration(
            connection_id=saved_connection.uuid,
            created_from=SOURCE_TYPE_CH_SUBSELECT,
            parameters=subsql_params,
            title=title,
        )
        assert source_id is not None

        # find without a title
        assert (
            saved_dataset.find_data_source_configuration(
                connection_id=saved_connection.uuid,
                created_from=SOURCE_TYPE_CH_SUBSELECT,
                parameters=subsql_params,
            )
            == source_id
        )

        # wrong created_from
        assert (
            saved_dataset.find_data_source_configuration(
                connection_id=saved_connection.uuid,
                created_from=SOURCE_TYPE_CH_TABLE,
                parameters=params,
                title=title,
            )
            is None
        )

        # wrong query
        assert (
            saved_dataset.find_data_source_configuration(
                connection_id=saved_connection.uuid,
                created_from=SOURCE_TYPE_CH_SUBSELECT,
                parameters=dict(subsql="SELECT 2 AS A"),
                title=title,
            )
            is None
        )

        # wrong title
        assert (
            saved_dataset.find_data_source_configuration(
                connection_id=saved_connection.uuid,
                created_from=SOURCE_TYPE_CH_SUBSELECT,
                parameters=subsql_params,
                title="Not my SQL",
            )
            is None
        )
