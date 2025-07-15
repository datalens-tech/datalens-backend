from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

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

        assert "test_data" in tmpl_schema_names  # for source MySQL server this is the database name
        assert len(set(tmpl_schema_names) & set(TRINO_SYSTEM_SCHEMAS)) == 0

        assert "test_mysql_catalog" in tmpl_db_names
        assert len(set(tmpl_db_names) & set(TRINO_SYSTEM_CATALOGS)) == 0


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
    def check_data_source_templates(
        self,
        conn: ConnectionTrino,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert not dsrc_templates

    def check_saved_connection(self, conn: ConnectionTrino, params: dict) -> None:
        assert conn.data.listing_sources is ListingSources.off
