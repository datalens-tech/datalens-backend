import pytest

from dl_core.exc import (
    InvalidRequestError,
    SourceDoesNotExist,
)
from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_trino.core.adapters import TRINO_SYSTEM_SCHEMAS
from dl_connector_trino.core.constants import (
    ListingSources,
    TrinoAuthType,
)
from dl_connector_trino.core.us_connection import (
    TRINO_SYSTEM_CATALOGS,
    ConnectionTrino,
)
from dl_connector_trino_tests.db.core.base import (
    BaseTrinoConnectionWithListingSourcesDisabled,
    BaseTrinoJwtTestClass,
    BaseTrinoPasswordTestClass,
    BaseTrinoTestClass,
)


class TestTrinoConnection(
    BaseTrinoTestClass,
    DefaultConnectionTestClass[ConnectionTrino],
):
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: ConnectionTrino, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.auth_type == TrinoAuthType.none
        assert conn.data.ssl_enable == params["ssl_enable"]
        assert conn.data.ssl_ca == params["ssl_ca"]

    def check_data_source_templates(
        self,
        conn: ConnectionTrino,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates

        tmpl_info = {(dsrc_tmpl.title, *dsrc_tmpl.group) for dsrc_tmpl in dsrc_templates}
        tmpl_titles, tmpl_db_names, tmpl_schema_names = zip(*tmpl_info)

        assert "sample" in tmpl_titles

        assert "default" in tmpl_schema_names
        assert len(set(tmpl_schema_names) & set(TRINO_SYSTEM_SCHEMAS)) == 0

        assert "test_memory_catalog" in tmpl_db_names
        assert len(set(tmpl_db_names) & set(TRINO_SYSTEM_CATALOGS)) == 0

    @pytest.fixture(scope="function")
    def catalog_name(self) -> str:
        return "test_memory_catalog"

    def test_get_db_names_and_tables(
        self, saved_connection: ConnectionTrino, catalog_name: str, sync_conn_executor_factory
    ) -> None:
        conn = saved_connection

        def sync_conn_executor_factory_for_conn(connection):
            return sync_conn_executor_factory()

        db_names = conn.get_db_names(sync_conn_executor_factory_for_conn)
        assert db_names

        tables = conn.get_tables(sync_conn_executor_factory_for_conn, db_name=catalog_name)
        assert tables

    def test_get_parameter_combinations(
        self, saved_connection: ConnectionTrino, catalog_name: str, sync_conn_executor_factory
    ) -> None:
        conn = saved_connection

        def sync_conn_executor_factory_for_conn(connection):
            return sync_conn_executor_factory()

        # Test with specific catalog
        param_combinations = conn.get_parameter_combinations(sync_conn_executor_factory_for_conn, db_name=catalog_name)
        assert param_combinations

        # All results should be from the specified catalog
        for combination in param_combinations:
            assert combination["db_name"] == catalog_name

        # Test search for "sample" table
        param_combinations = conn.get_parameter_combinations(
            sync_conn_executor_factory_for_conn, db_name=catalog_name, search_text="ampl"
        )
        assert param_combinations
        assert all("ampl" in i["table_name"] for i in param_combinations)

        # Test with offset
        offset = 2
        param_combinations_with_offset = conn.get_parameter_combinations(
            sync_conn_executor_factory_for_conn,
            db_name=catalog_name,
            offset=offset,
        )
        param_combinations_without_offset = conn.get_parameter_combinations(
            sync_conn_executor_factory_for_conn,
            db_name=catalog_name,
        )
        if len(param_combinations_without_offset) >= offset:
            assert len(param_combinations_without_offset) == len(param_combinations_with_offset) + offset
        else:
            assert len(param_combinations_without_offset) >= len(param_combinations_with_offset) == 0

        # Test with limit
        param_combinations = conn.get_parameter_combinations(
            sync_conn_executor_factory_for_conn,
            db_name=catalog_name,
            limit=1,
        )
        assert len(param_combinations) == 1

    def test_get_source_templates_paginated(
        self, saved_connection: ConnectionTrino, catalog_name: str, sync_conn_executor_factory
    ) -> None:
        conn = saved_connection

        def sync_conn_executor_factory_for_conn(connection):
            return sync_conn_executor_factory()

        # Test basic functionality compared to the old method
        templates = conn.get_data_source_templates_paginated(sync_conn_executor_factory_for_conn)
        old_templates = conn.get_data_source_templates(sync_conn_executor_factory_for_conn)
        assert templates == old_templates

        # Test search functionality
        templates_with_offset = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn, db_name=catalog_name, search_text="ampl", limit=1, offset=1
        )
        templates_with_only_limit = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn,
            db_name=catalog_name,
            search_text="ampl",
            limit=2,
        )
        assert len(templates_with_offset) == len(templates_with_only_limit) - 1
        assert templates_with_offset == templates_with_only_limit[1:]
        assert all("ampl" in template.title for template in templates_with_only_limit)

        # Search without db_name - should fail for Trino
        with pytest.raises(InvalidRequestError, match="db_name parameter is required when search_text is provided"):
            conn.get_data_source_templates_paginated(
                sync_conn_executor_factory_for_conn,
                search_text="some_search_term"
                # db_name is intentionally omitted
            )
        with pytest.raises(InvalidRequestError, match="db_name parameter is required when offset > 0 is provided"):
            conn.get_data_source_templates_paginated(
                sync_conn_executor_factory_for_conn,
                offset=1
                # db_name is intentionally omitted
            )

        # Test with invalid/non-existent catalog name
        with pytest.raises(SourceDoesNotExist):
            conn.get_data_source_templates_paginated(
                sync_conn_executor_factory_for_conn, db_name="non_existent_catalog_name_12345"
            )

        # Test search with non-existent pattern
        templates = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn,
            db_name=catalog_name,
            search_text="non_existent_pattern_xyz123",
        )
        assert len(templates) == 0

        # Test with None and empty search_text (should be equivalent to no search)
        templates_none = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn, db_name=catalog_name, search_text=None
        )
        templates_no_search = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn, db_name=catalog_name
        )
        templates_empty_search = conn.get_data_source_templates_paginated(
            sync_conn_executor_factory_for_conn, db_name=catalog_name, search_text=""
        )
        assert templates_none == templates_no_search == templates_empty_search


class TestTrinoPasswordConnection(BaseTrinoPasswordTestClass, TestTrinoConnection):
    def check_saved_connection(self, conn: ConnectionTrino, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.password == params["password"]
        assert conn.data.auth_type == TrinoAuthType.password
        assert conn.data.ssl_enable == params["ssl_enable"]
        assert conn.data.ssl_ca == params["ssl_ca"]


class TestTrinoJwtConnection(BaseTrinoJwtTestClass, TestTrinoConnection):
    def check_saved_connection(self, conn: ConnectionTrino, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.host == params["host"]
        assert conn.data.port == params["port"]
        assert conn.data.username == params["username"]
        assert conn.data.jwt == params["jwt"]
        assert conn.data.auth_type == TrinoAuthType.jwt
        assert conn.data.ssl_enable == params["ssl_enable"]
        assert conn.data.ssl_ca == params["ssl_ca"]


class TestTrinoConnectionWithListingSourcesDisabled(
    BaseTrinoConnectionWithListingSourcesDisabled,
    TestTrinoConnection,
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            TestTrinoConnection.test_get_parameter_combinations: "Not applicable",
            TestTrinoConnection.test_get_source_templates_paginated: "Not applicable",
        }
    )

    def check_data_source_templates(
        self,
        conn: ConnectionTrino,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert not dsrc_templates

    def check_saved_connection(self, conn: ConnectionTrino, params: dict) -> None:
        assert conn.data.listing_sources is ListingSources.off
