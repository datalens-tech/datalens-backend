from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Any, Dict

import logging

import attr
from aiohttp.client import ClientResponse
from aiohttp.web import HTTPBadRequest
from datetime import datetime
from urllib.parse import urljoin

from bi_constants.enums import ConnectionType

from bi_core.connectors.solomon_base.adapter import AsyncBaseSolomonAdapter
from bi_core.exc import DatabaseQueryError

if TYPE_CHECKING:
    from bi_connector_monitoring.core.target_dto import MonitoringConnTargetDTO
    from bi_core.connection_executors.models.db_adapter_data import DBAdapterQuery


LOGGER = logging.getLogger(__name__)


class AsyncMonitoringAdapter(AsyncBaseSolomonAdapter):
    conn_type: ClassVar[ConnectionType] = ConnectionType.monitoring
    _target_dto: MonitoringConnTargetDTO = attr.ib()

    def __attrs_post_init__(self) -> None:
        self._url = f'https://{self._target_dto.host}'
        super().__attrs_post_init__()

    def get_session_headers(self) -> Dict[str, str]:
        default_headers = {
            'Content-Type': 'application/json',
        }
        auth_headers = {
            'Authorization': f'Bearer {self._target_dto.iam_token}',
        }
        return {
            **super().get_session_headers(),
            **default_headers,
            **auth_headers,
        }

    async def run_query(self, dba_query: DBAdapterQuery) -> ClientResponse:
        req_params = {'from', 'to'}
        conn_params = dba_query.connector_specific_params
        if conn_params is None or not req_params <= set(conn_params):
            db_exc = self.make_exc(
                status_code=HTTPBadRequest.status_code,
                err_body="'from', 'to' must be in parameters",
                debug_compiled_query=dba_query.debug_compiled_query,
            )
            raise db_exc

        for param in ('from', 'to'):
            conn_param = conn_params[param]
            if isinstance(conn_param, datetime):
                conn_params[param] = int(conn_param.timestamp()) * 1000

        query_text = self.compile_query_for_execution(dba_query.query)
        resp = await self._session.post(
            url=urljoin(
                self._url,
                f'{self._target_dto.url_path}/data/read?folderId={self._target_dto.folder_id}',
            ),
            json={
                'query': query_text,
                'fromTime': conn_params['from'],
                'toTime': conn_params['to'],
            },
        )
        return resp

    @staticmethod
    def _parse_response_body_data(data: list) -> dict:
        labels = set()
        with_alias = False
        for chunk in data:
            labels.update(chunk['labels'].keys())
            alias = chunk.get('name')
            if alias:
                with_alias = True
        ordered_labels = tuple(labels)
        if with_alias:
            ordered_labels = ordered_labels + ('_alias',)

        schema = [
            ('timestamp', 'datetime'), ('value', 'float')
        ] + [
            (label, 'string') for label in ordered_labels
        ]

        rows = []
        for chunk in data:
            timeseries = chunk['timeseries']
            if not timeseries.get('timestamps'):
                continue

            label_values = [chunk['labels'].get(label, '') for label in ordered_labels if label != '_alias']
            if with_alias:
                label_values.append(chunk.get('name', ''))

            values = timeseries.get('doubleValues') or timeseries.get('int64Values')
            for (ts, v) in zip(timeseries['timestamps'], values):
                row = [datetime.fromtimestamp(ts / 1000), float(v)] + label_values
                rows.append(row)

        return dict(rows=rows, schema=schema)

    def parse_response_body(self, response: Dict[str, Any], dba_query: DBAdapterQuery) -> dict:
        data = response.get('metrics', [])

        try:
            return self._parse_response_body_data(data)
        except (ValueError, KeyError) as err:
            raise DatabaseQueryError(
                message=f'Unexpected API response body: {err.args[0]}',
                db_message=response.get('message', 'unknown error'),
                query=dba_query.debug_compiled_query,
            )
