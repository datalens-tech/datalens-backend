from __future__ import annotations

import pytest

from bi_external_api.docs.main_dc import DoubleCloudDocsBuilder
from bi_external_api.domain import external as ext
from bi_external_api.workbook_ops.facade import WorkbookOpsFacade

from .test_acceptance import (
    AcceptanceScenario,
    CreatedConnectionTestingData,
)


class AcceptanceScenarioDC(AcceptanceScenario):
    @pytest.fixture()
    def wb_title(self) -> str:
        raise NotImplementedError()

    # Final fixtures. MUST NOT be overridden
    @pytest.fixture()
    async def wb_id(self, api, wb_title) -> str:
        rs = await api.create_dc_workbook(ext.TrueWorkbookCreateRequest(workbook_title=wb_title))
        return rs.workbook_id

    @pytest.fixture(
        params=[
            "ch_connection",
        ]
    )
    def conn_td(self, request) -> ext.EntryInfo:
        """
        Dispatching fixture that return any connection
        """
        return request.getfixturevalue(request.param)

    # TODO FIX: Check full examples via HTTP-API
    @pytest.mark.asyncio
    async def test_docs_sample_workbook(
        self,
        api: WorkbookOpsFacade,
        ch_connection: CreatedConnectionTestingData,
        wb_id: str,
    ):
        docs = DoubleCloudDocsBuilder()
        wb = docs.get_sample_workbook(ch_connection.entry_info.name)

        await api.write_workbook(
            ext.WorkbookWriteRequest(
                workbook=wb,
                workbook_id=wb_id,
                force_rewrite=False,
            )
        )
