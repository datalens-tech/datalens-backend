from __future__ import annotations

import asyncio
import functools
import logging
import operator
import random
from datetime import datetime
from typing import List, TYPE_CHECKING

from aiohttp import web

from bi_alerts.utils.charts import ChartsClient, rebuild_chart_data
from bi_alerts.settings import APP_KEY_SETTINGS
from bi_alerts.utils.solomon import TvmCliSingletonSolomonFetcher
from bi_core.us_manager.crypto.main import EncryptedData   # type: ignore  # TODO: fix

if TYPE_CHECKING:
    from bi_alerts.models.db import BIDatasync
    from bi_core.us_manager.crypto.main import CryptoController

from .. import schemas as s
from ..db_manager import BIDatasyncManager


LOGGER = logging.getLogger(__name__)


CHART_OBJ_ID = '8gc2comg44f06'
CHART_OBJ_PARAMS = dict(
    type='share',
    date='__relative_-0d',
    country='RU',
    lateness='1',
    device_type='desktop',
    search_engine='yandex',
)


class ChartsTestData(web.View):
    async def get(self) -> web.Response:
        settings = self.request.app[APP_KEY_SETTINGS]
        charts_client = ChartsClient(settings.CHARTS_BASE_URL, settings.CHARTS_TOKEN)
        chart_data = await charts_client.fetch_editor_data(
            id=CHART_OBJ_ID,
            params=CHART_OBJ_PARAMS,
        )
        await charts_client.close()

        range_limit = 3600 * 12
        ts = int(datetime.utcnow().timestamp())

        return web.json_response({
            'metrics': [
                {
                    'labels': {'chart_id': 'my_chart_1', 'params': 'hash_from_params_1'},
                    'timeseries': [
                        {
                            'ts': ts - range_limit + offset + 1,
                            'value': random.randint(30, 70)
                        } for offset in range(range_limit)
                    ]
                },
                {
                    'labels': {'chart_id': 'my_chart_2', 'params': 'hash_from_params_2'},
                    'timeseries': [
                        {
                            'ts': ts - range_limit + offset + 1,
                            'value': random.randint(40, 60)
                        } for offset in range(range_limit)
                    ]
                },
                *rebuild_chart_data(CHART_OBJ_ID, CHART_OBJ_PARAMS, chart_data)
            ]
        })


class ChartsData(web.View):
    async def get(self) -> web.Response:
        settings = self.request.app[APP_KEY_SETTINGS]
        data = s.BIChartDataSchema().load(self.request.rel_url.query)
        tvm_cli = await TvmCliSingletonSolomonFetcher.get_tvm_cli()
        st = tvm_cli.check_service_ticket(self.request.headers.get('X-Ya-Service-Ticket'))
        if int(st.src) != settings.SOLOMON_FETCHER_TVM_ID.value:
            raise web.HTTPForbidden(reason='Access denied (service)')

        async with self.request.app.db.acquire() as conn:  # type: ignore  # TODO: fix
            datasyncs = await BIDatasyncManager(conn).get_top_for_service(
                service=data['service_name'],
                limit=settings.DATASYNC_SHARD_LIMIT
            )

        semaphore = asyncio.Semaphore(settings.CHARTS_CHECKER_SEMAPHORE)

        async def get_charts_data_with_semaphore(
            datasync: BIDatasync, crypto: CryptoController
        ) -> List[dict]:
            encrypted_data = EncryptedData(
                key_id=datasync.crypto_key_id,
                key_kind='local_fernet',
                cypher_text=datasync.oauth_encrypted,
            )
            token = crypto.decrypt(encrypted_data)
            charts_client = ChartsClient(
                settings.CHARTS_BASE_URL,
                token or settings.CHARTS_TOKEN,
            )
            LOGGER.info('Get data from: %s, %s', datasync.chart_id, datasync.chart_params_hash)
            async with semaphore:
                chart_data = await charts_client.fetch_editor_data(
                    id=datasync.chart_id,
                    params=datasync.chart_params,  # type: ignore  # TODO: fix
                )
            await charts_client.close()
            return rebuild_chart_data(
                datasync.chart_id, datasync.chart_params, chart_data, datasync.time_shift,  # type: ignore  # TODO: fix
            )

        tasks = [
            asyncio.ensure_future(
                get_charts_data_with_semaphore(datasync, self.request.app.crypto.copy())  # type: ignore  # TODO: fix
            )
            for datasync in datasyncs
        ] if datasyncs is not None else []

        results = await asyncio.gather(*tasks)

        last_check_time = datetime.utcnow()
        async with self.request.app.db.acquire() as conn:  # type: ignore  # TODO: fix
            for datasync in datasyncs:  # type: ignore  # TODO: fix
                await BIDatasyncManager(conn).update(
                    datasync.id,
                    last_check_time=last_check_time,
                )

        return web.json_response({
            'metrics': functools.reduce(operator.iconcat, results, [])
        })
