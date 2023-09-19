from typing import (
    Callable,
    Generic,
    TypeVar,
)

import pytest

from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core.exc import SourceDoesNotExist
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core_testing.testcases.data_source import BaseDataSourceTestClass

from bi_connector_bundle_partners.base.core.data_source import PartnersCHDataSourceBase
from bi_connector_bundle_partners.base.core.us_connection import PartnersCHConnectionBase
import bi_connector_bundle_partners_tests.db.config as test_config


_CONN_TV = TypeVar("_CONN_TV", bound=PartnersCHConnectionBase)
_DSRC_SPEC_TV = TypeVar("_DSRC_SPEC_TV", bound=StandardSQLDataSourceSpec)
_DSRC_TV = TypeVar("_DSRC_TV", bound=PartnersCHDataSourceBase)


class PartnersDataSourceTestClass(
    BaseDataSourceTestClass[_CONN_TV, _DSRC_SPEC_TV, _DSRC_TV],
    Generic[_CONN_TV, _DSRC_SPEC_TV, _DSRC_TV],
):
    def test_data_source(
        self,
        data_source: _DSRC_TV,
        sync_conn_executor_factory: Callable[[], SyncConnExecutorBase],
        conn_default_service_registry: ServicesRegistry,
    ) -> None:
        dsrc = data_source
        assert dsrc.conn_type == self.conn_type
        assert dsrc.db_name == test_config.DB_NAME
        assert dsrc.table_name == test_config.TABLE_NAME

        assert dsrc.source_exists(conn_executor_factory=sync_conn_executor_factory)
        assert dsrc.get_schema_info(conn_executor_factory=sync_conn_executor_factory).schema

        dsrc.spec.db_name = "some_database"
        with pytest.raises(SourceDoesNotExist):
            _ = dsrc.get_filters(service_registry=conn_default_service_registry)
