from __future__ import annotations

import asyncio
import io
import logging
import typing
from typing import BinaryIO

from aiohttp import web
from aiohttp.multipart import BodyPartReader
import openpyxl
import openpyxl.cell
import openpyxl.worksheet


LOGGER = logging.getLogger(__name__)


def custom_get_cell(
    sheet: openpyxl.worksheet.Worksheet,
) -> typing.Callable[[typing.Any, typing.Any], openpyxl.cell.Cell | None]:
    def get_cell(row: typing.Any, column: typing.Any) -> openpyxl.cell.Cell | None:
        """
        Custom _get_cell implementation without saving wrappers for null to sheet.
        Motivation: when null (empty) value is read from sheet in original function,
        it is saved to sheet cells duting read iteration over rows. This causes
        excessive memory usage for sheets with many empty cells.

        Tested on openpyxl==3.0.10.
        """
        if not 0 < row < 1048577:
            raise ValueError(f"Row numbers must be between 1 and 1048576. Row number supplied was {row}")
        coordinate = (row, column)
        if not coordinate in sheet._cells:
            return openpyxl.cell.Cell(sheet, row=row, column=column)

        return sheet._cells[coordinate]

    return get_cell


def parse_excel_data(data: BinaryIO) -> list:
    result = []
    try:
        wb = openpyxl.load_workbook(data, data_only=True, keep_links=False)
    except Exception:
        raise web.HTTPUnprocessableEntity(reason="Invalid excel file")
    for sheetname in wb.sheetnames:
        sheet = wb[sheetname]
        sheet._get_cell = custom_get_cell(sheet)

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
