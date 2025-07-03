import copy

from dl_constants.enums import (
    DataSourceType,
    RawSQLLevel,
)
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset
from dl_core_tests.db.base import DefaultCoreTestClass

from dl_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE


FAKE_CREATED_FROM = DataSourceType.declare("FAKE_SOURCE")


class TestDataSource(DefaultCoreTestClass):
    raw_sql_level = RawSQLLevel.subselect

    def test_find_data_source_configuration_default(
        self,
        saved_connection: ConnectionBase,
        saved_dataset: Dataset,
        dsrc_params: dict,
    ):
        source_id = saved_dataset.find_data_source_configuration(
            connection_id=saved_connection.uuid,
            created_from=SOURCE_TYPE_CH_TABLE,
            parameters=dsrc_params,
            title=None,
        )
        assert source_id is not None

    def test_find_data_source_configuration_wrong_title(
        self,
        saved_connection: ConnectionBase,
        saved_dataset: Dataset,
        dsrc_params: dict,
    ):
        source_id = saved_dataset.find_data_source_configuration(
            connection_id=saved_connection.uuid,
            created_from=SOURCE_TYPE_CH_TABLE,
            parameters=dsrc_params,
            title="Wrong title",
        )
        assert source_id is None

    def test_find_data_source_configuration_wrong_connection(
        self,
        saved_dataset: Dataset,
        dsrc_params: dict,
    ):
        source_id = saved_dataset.find_data_source_configuration(
            connection_id="wrong_connection_id",
            created_from=SOURCE_TYPE_CH_TABLE,
            parameters=dsrc_params,
            title=None,
        )
        assert source_id is None

    def test_find_data_source_configuration_wrong_created_from(
        self,
        saved_connection: ConnectionBase,
        saved_dataset: Dataset,
        dsrc_params: dict,
    ):
        source_id = saved_dataset.find_data_source_configuration(
            connection_id=saved_connection.uuid,
            created_from=FAKE_CREATED_FROM,
            parameters=dsrc_params,
            title=None,
        )
        assert source_id is None

    def test_find_data_source_configuration_wrong_parameters(
        self,
        saved_connection: ConnectionBase,
        saved_dataset: Dataset,
        dsrc_params: dict,
    ):
        dsrc_params = copy.deepcopy(dsrc_params)
        dsrc_params["table_name"] = "wrong_table_name"

        source_id = saved_dataset.find_data_source_configuration(
            connection_id=saved_connection.uuid,
            created_from=SOURCE_TYPE_CH_TABLE,
            parameters=dsrc_params,
            title=None,
        )
        assert source_id is None
