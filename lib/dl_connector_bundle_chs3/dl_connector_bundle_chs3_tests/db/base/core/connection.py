from __future__ import annotations

import abc

from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from dl_connector_bundle_chs3_tests.db.base.core.base import (
    FILE_CONN_TV,
    BaseCHS3TestClass,
)


class CHS3ConnectionTestBase(
    BaseCHS3TestClass,
    DefaultConnectionTestClass[FILE_CONN_TV],
    metaclass=abc.ABCMeta,
):
    do_check_data_export_flag = False

    def check_saved_connection(self, conn: FILE_CONN_TV, params: dict) -> None:
        assert set(src.title for src in conn.data.sources) == set(src.title for src in params["sources"])

    def check_data_source_templates(
        self,
        conn: FILE_CONN_TV,
        dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            conn_src = conn.get_file_source_by_id(dsrc_tmpl.parameters["origin_source_id"])
            assert conn_src.title == dsrc_tmpl.title
