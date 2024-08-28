from __future__ import annotations

import asyncio
import io
import logging
from typing import (
    BinaryIO,
    List,
)

from aiohttp import web
from aiohttp.multipart import BodyPartReader
from openpyxl import load_workbook


LOGGER = logging.getLogger(__name__)


def parse_excel_data(data: BinaryIO) -> List:
    result = []
    try:
        wb = load_workbook(data, data_only=True, keep_links=False)
    except Exception:
        raise web.HTTPUnprocessableEntity(reason="Invalid excel file")
    for sheetname in wb.sheetnames:
        sheet = wb[sheetname]
        result.append(
            {
                "sheetname": sheetname,
                "data": [
                    [
                        {
                            "value": str(cell.value) if cell.data_type == "d" else cell.value,
                            "data_type": "i" if isinstance(cell.value, int) else cell.data_type,
                        }
                        for cell in row
                    ]
                    for row in sheet.rows
                ],
            }
        )
    return result


class ReaderView(web.View):
    async def post(self) -> web.StreamResponse:
        loop = asyncio.get_running_loop()
        reader = await self.request.multipart()
        field = await reader.next()
        assert isinstance(field, BodyPartReader)
        assert field.name == "file"

        data = io.BytesIO(bytes(await field.read()))

        tpe = self.request.app["tpe"]
        result = await loop.run_in_executor(tpe, parse_excel_data, data)

        return web.json_response(data=result)
