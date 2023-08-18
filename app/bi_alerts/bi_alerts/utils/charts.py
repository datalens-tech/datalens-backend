from __future__ import annotations

import aiohttp
import json
import logging
from typing import Any, Dict, List
from hashlib import sha256, md5


LOGGER = logging.getLogger(__name__)


def hash_dict(item: dict) -> str:
    frozen_params = {k: str(v) for k, v in item.items()}
    return sha256(json.dumps(frozen_params, sort_keys=True).encode()).hexdigest()


def hash_str(item: str) -> str:
    return md5(item.encode()).hexdigest()


def hash_list(item: list) -> str:
    return sha256(json.dumps(sorted(item)).encode()).hexdigest()


def charts_data_generator(data, categories_ms, time_shift):  # type: ignore  # TODO: fix
    if isinstance(data[0], dict):
        for row in data:
            if row['x'] is None:
                continue
            yield int(row['x'] / 1000) + time_shift, row['y']
    elif isinstance(data[0], list):
        for row in data:
            if row[0] is None:
                continue
            yield int(row[0] / 1000) + time_shift, row[1]
    elif categories_ms:
        for x, y in zip(categories_ms, data):
            if x is None:
                continue
            yield int(x / 1000) + time_shift, y
    else:
        raise ValueError('Unknown chart format')


def rebuild_chart_data(
    chart_id: str, chart_params: dict, chart_data: dict, time_shift: int = 0,
) -> List[dict]:
    hashed_params = hash_dict(chart_params)
    if not chart_data:
        LOGGER.info(
            (
                f'There is no data in chart,'
                f' id: {chart_id}, params: {hashed_params}'
            )
        )
        return []
    data = chart_data['data']
    categories_ms = None

    if isinstance(data, dict):
        if 'categories' in data:
            LOGGER.info(
                (
                    f'Categorical graphs are not supported,'
                    f' id: {chart_id}, params: {hashed_params}'
                )
            )
            return []
        categories_ms = data.get('categories_ms')
        data = data.get('graphs', [])

    payload = [{
        'name': line.get('id') or line.get('name') or line.get('title') or '__EMPTY__',
        'labels': {
            'chart_id': chart_id,
            'params': hashed_params,
            'yaxis': line.get('yAxis', 0),
        },
        'timeseries': [
            {
                'ts': ts, 'value': value,
            } for (ts, value) in charts_data_generator(line['data'], categories_ms, time_shift) if value is not None
        ]
    } for line in data if line['data']]

    return payload


class ChartsClient:

    _logger = LOGGER

    def __init__(self, url: str, token: str, connect_timeout: int = 30, read_timeout: int = 600):
        self._url = url
        self._session = aiohttp.ClientSession(
            headers={
                'Authorization': 'OAuth {}'.format(token),
                'Content-Type': 'application/json; charset=utf-8',
                'User-Agent': f'{aiohttp.http.SERVER_SOFTWARE} DataLens Alerts',
            },
            timeout=aiohttp.ClientTimeout(
                sock_connect=connect_timeout,
                sock_read=read_timeout,
            )
        )

    async def close(self) -> None:
        return await self._session.close()

    async def fetch_editor_data(self, id: str, params: Dict[str, Any]):  # type: ignore  # TODO: fix
        data = json.dumps({'id': id, 'params': params})
        async with self._session.post(self._url, data=data) as resp:
            if resp.status >= 400:
                test = await resp.text()
                self._logger.info(
                    (
                        f'Received error from alerts API. status {resp.status},'
                        f' headers: {resp.headers}, response: {test}'
                    )
                )
                response = {}
            else:
                try:
                    response = await resp.json()
                except aiohttp.ContentTypeError as exc:
                    self._logger.info(f'{exc.message} for chart data: {data}')
                    response = {}

        return response
