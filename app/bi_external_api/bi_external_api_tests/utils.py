from contextlib import asynccontextmanager
from uuid import uuid4

from bi_external_api.domain import external as ext


@asynccontextmanager
async def tmp_wb_id(client, project_id: str) -> str:
    wb_title = uuid4().hex
    resp = await client.create_true_workbook(
        ext.DCOpWorkbookCreateRequest(
            workbook_title=wb_title,
            project_id=project_id,
        )
    )
    wb_id = resp.workbook_id
    yield wb_id
    await client.delete_workbook(
        ext.WorkbookDeleteRequest(
            workbook_id=wb_id,
        )
    )
