from __future__ import annotations

import asyncio
import io
import logging
from typing import (
    Any,
    BinaryIO,
)

from aiohttp import web
from aiohttp.multipart import BodyPartReader
import frozendict
import openpyxl
import openpyxl.cell.cell

from dl_file_secure_reader_lib.settings import FileSecureReaderSettings


LOGGER = logging.getLogger(__name__)


class CachedCellProcessor:
    """
    Cache wrappers for cell values to prevent excessive memory usage when excel
    file is made of duplicate values.
    """

    def __init__(self) -> None:
        self.cache_values: dict[tuple[str, Any], Any] = {}

    def process_cell(self, cell: openpyxl.cell.cell.Cell) -> dict:
        if cell.data_type == "d":
            cell_value = str(cell.value)
        else:
            cell_value = cell.value

        if isinstance(cell.value, int):
            cell_type = "i"
        else:
            cell_type = cell.data_type

        cache_key = (cell_type, cell_value)

        if cache_key in self.cache_values:
            cache_value = self.cache_values[cache_key]
        else:
            cache_value = frozendict.frozendict(
                data_type=cell_type,
                value=cell_value,
            )
            self.cache_values[cache_key] = cache_value

        return cache_value


def parse_excel_data(data: BinaryIO, feature_excel_read_only: bool) -> list:
    result = []

    cell_processor = CachedCellProcessor()

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
                "data": [[cell_processor.process_cell(cell) for cell in row] for row in sheet.rows],
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
