from __future__ import annotations

import aiohttp
import pytest


@pytest.mark.asyncio
async def test_reader(web_app, excel_data):
    with aiohttp.MultipartWriter() as mpwriter:
        part = mpwriter.append(excel_data)
        part.set_content_disposition('form-data', name='file')
        resp = await web_app.post('/reader/excel', data=mpwriter)

    assert resp.status == 200
    result = await resp.json()
    assert len(result) == 3
    spreadsheet = result[2]
    assert spreadsheet['sheetname'] == 'People'
    assert spreadsheet['data'][0] == [
        {'value': 'Person', 'data_type': 's'},
        {'value': 'Region', 'data_type': 's'},
    ]
