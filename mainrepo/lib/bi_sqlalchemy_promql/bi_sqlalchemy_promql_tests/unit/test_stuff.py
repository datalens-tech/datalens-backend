from __future__ import annotations

import sqlalchemy as sa
from datetime import datetime

from bi_sqlalchemy_promql.cli import SyncPromQLClient, rebuild_prometheus_data
from bi_sqlalchemy_promql.errors import InterfaceError


def test_engine(sa_engine):
    conn = sa_engine.connect()
    assert conn


class MockupSyncPromQLClient(SyncPromQLClient):
    def query_range(self, query, parameters):
        req_params = {'start', 'end', 'step'}
        if parameters is None or not req_params <= set(parameters):
            raise InterfaceError(f'{req_params} must be in parameters')

        data = {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {
                            "__name__": "up",
                            "instance": "cadvisor:8080",
                            "job": "cadvisor",
                        },
                        "values": [
                            [1628445300, "1"],
                            [1628445600, "1"],
                        ],
                    },
                    {
                        "metric": {
                            "__name__": "up",
                            "instance": "localhost:9090",
                            "job": "prometheus",
                        },
                        "values": [
                            [1628445300, "1"],
                            [1628445600, "1"],
                        ],
                    },
                ],
            },
        }
        result = rebuild_prometheus_data(data['data'])
        return result


def test_result(engine_url):
    sa_engine = sa.create_engine(
        engine_url,
        connect_args=dict(
            cli_cls=MockupSyncPromQLClient,
        ),
    )
    res = sa_engine.execute(
        'up',
        {
            'start': '2021-08-08T17:55:00Z',
            'end': '2021-08-08T18:00:00Z',
            'step': '5m'
        },
    )
    description = res.cursor.description
    assert isinstance(description, tuple)
    cols = tuple((col[0], col[1]) for col in description)
    assert cols == (
        ('timestamp', 'unix_timestamp'), ('value', 'float64'),
        ('__name__', 'string'), ('instance', 'string'), ('job', 'string'),
    )
    data = list(res)
    assert data == [
        (datetime(2021, 8, 8, 17, 55), 1., 'up', 'cadvisor:8080', 'cadvisor'),
        (datetime(2021, 8, 8, 18, 0), 1., 'up', 'cadvisor:8080', 'cadvisor'),
        (datetime(2021, 8, 8, 17, 55), 1., 'up', 'localhost:9090', 'prometheus'),
        (datetime(2021, 8, 8, 18, 0), 1., 'up', 'localhost:9090', 'prometheus'),
    ]
