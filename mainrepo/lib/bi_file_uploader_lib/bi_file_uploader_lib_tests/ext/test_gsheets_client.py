import logging
import os

import pytest

from bi_core.aio.web_app_services.gsheets import (
    GSheetsSettings,
    NumberFormatType,
)
from bi_file_uploader_lib.gsheets_client import GSheetsClient
from bi_utils.aio import ContextVarExecutor

LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_number_format_types(env_param_getter, redis_model_manager):
    gsheets_settings = GSheetsSettings(
        api_key=env_param_getter.get_str_value("GOOGLE_API_KEY"),
        client_id="dummy",
        client_secret="dummy",
    )
    spreadsheet_id = "1vTeWZz2M8c3bvTdRIrqsNhN9m4YwjUguwAnAqBahxi0"
    auth = None

    with ContextVarExecutor(max_workers=min(32, os.cpu_count() * 3 + 4)) as tpe:
        async with GSheetsClient(settings=gsheets_settings, auth=auth, tpe=tpe) as sheets_client:
            spreadsheet = await sheets_client.get_spreadsheet_sample(
                spreadsheet_id=spreadsheet_id,
            )

    for sheet in spreadsheet.sheets[1:]:  # the first sheet contains test data
        data = sheet.data
        header = data[0]  # the first row contains expected number format types
        expected_types = [NumberFormatType(cell.value.upper()) for cell in header]
        for row_idx, row in enumerate(data[1:]):
            for col_idx, (cell, expected_fmt) in enumerate(zip(row, expected_types)):
                assert cell.number_format_type == expected_fmt, f"Failed at [{row_idx + 1}, {col_idx}], {cell=}"
        LOGGER.info(f"{sheet.title} is OK")
