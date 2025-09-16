from __future__ import annotations

import asyncio
import io
import logging
from typing import BinaryIO

from aiohttp import web
from aiohttp.multipart import BodyPartReader
import openpyxl
import openpyxl.cell.cell

from dl_file_secure_reader_lib.settings import FileSecureReaderSettings


LOGGER = logging.getLogger(__name__)


def parse_excel_data(data: BinaryIO, feature_excel_read_only: bool) -> list:
    result = []

    # Reuse null value
    null_value = {
        "value": None,
        "data_type": openpyxl.cell.cell.TYPE_NULL,
    }

    try:
        wb = openpyxl.load_workbook(
            data,
            data_only=True,
            keep_links=False,
            read_only=feature_excel_read_only,
        )
    except Exception:
        raise web.HTTPUnprocessableEntity(reason="Invalid excel file")
    for sheetname in wb.sheetnames:
        sheet = wb[sheetname]
        if feature_excel_read_only:
            sheet.reset_dimensions()
            sheet.calculate_dimension(force=True)

        result.append(
            {
                "sheetname": sheetname,
                "data": [
                    [
                        null_value
                        if cell.value is None
                        else {
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
        settings: FileSecureReaderSettings = self.request.app["settings"]
        result = await loop.run_in_executor(tpe, parse_excel_data, data, settings.FEATURE_EXCEL_READ_ONLY)

        return web.json_response(data=result)
