from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Any, Dict

import logging

import attr
import tvmauth
from aiohttp.client import ClientResponse
from aiohttp.web import HTTPBadRequest
from datetime import datetime
from urllib.parse import urljoin

from bi_constants.enums import ConnectionType

from bi_blackbox_client.authenticate import get_user_ticket_header
from bi_blackbox_client.tvm import get_tvm_headers
from bi_connector_solomon.core.tvm import TvmCliSingletonSolomon, get_solomon_tvm_destination
from bi_connector_monitoring.core.adapter_base import AsyncBaseSolomonAdapter
from bi_core.exc import DatabaseQueryError

if TYPE_CHECKING:
    from bi_connector_solomon.core.target_dto import SolomonConnTargetDTO
    from bi_core.connection_executors.models.db_adapter_data import DBAdapterQuery


LOGGER = logging.getLogger(__name__)


class AsyncSolomonAdapter(AsyncBaseSolomonAdapter):
    conn_type: ClassVar[ConnectionType] = ConnectionType.solomon
    _target_dto: SolomonConnTargetDTO = attr.ib()
    _tvm_cli: tvmauth.TvmClient = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._tvm_cli = TvmCliSingletonSolomon.get_tvm_cli_sync()
        self._url = f'https://{self._target_dto.host}'
        super().__attrs_post_init__()

    def get_session_headers(self) -> Dict[str, str]:
        default_headers = {
            'Content-Type': 'application/json',
        }
        tvm_headers = get_tvm_headers(
            tvm_cli=self._tvm_cli, destination=get_solomon_tvm_destination(self._url),
        )
        auth_headers = get_user_ticket_header(
            self._target_dto.cookie_session_id,
            self._target_dto.cookie_sessionid2,
            userip=self._target_dto.user_ip,
            host=self._target_dto.user_host,
        )
        return {
            **super().get_session_headers(),
            **default_headers,
            **auth_headers,
            **tvm_headers,
        }

    async def run_query(self, dba_query: DBAdapterQuery) -> ClientResponse:
        req_params = {'project_id', 'from', 'to'}
        conn_params = dba_query.connector_specific_params
        if conn_params is None or not req_params <= set(conn_params):
            db_exc = self.make_exc(
                status_code=HTTPBadRequest.status_code,
                err_body="'project_id', 'from', 'to' must be in parameters",
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
                'api/v2/projects/{project_id}/sensors/data'.format(
                    project_id=conn_params['project_id'],
                ),
            ),
            json={
                'program': query_text,
                'from': conn_params['from'],
                'to': conn_params['to'],
            },
        )
        return resp

    @staticmethod
    def _parse_response_body_data(data: list) -> dict:
        labels = set()
        with_alias = False
        for chunk in data:
            timeseries = chunk['timeseries']
            labels.update(timeseries['labels'].keys())
            alias = timeseries.get('alias')
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

            label_values = [timeseries['labels'].get(label, '') for label in ordered_labels if label != '_alias']
            if with_alias:
                label_values.append(timeseries.get('alias', ''))

            for (ts, v) in zip(timeseries['timestamps'], timeseries['values']):
                row = [datetime.fromtimestamp(ts / 1000), float(v)] + label_values
                rows.append(row)

        return dict(rows=rows, schema=schema)

    def parse_response_body(self, response: Dict[str, Any], dba_query: DBAdapterQuery) -> dict:
        if 'vector' in response:
            data = response['vector']
        elif 'timeseries' in response:
            data = [response]
        else:
            data = []

        try:
            return self._parse_response_body_data(data)
        except (ValueError, KeyError) as err:
            raise DatabaseQueryError(
                message=f'Unexpected API response body: {err.args[0]}',
                db_message=response.get('message', 'unknown error'),
                query=dba_query.debug_compiled_query,
            )
