from __future__ import annotations

import pytest

from bi_alerts.utils.charts import rebuild_chart_data, hash_dict


solomon_data_format = [
    {
        'name': 'line_id_1',
        'labels': {
            'chart_id': 'wieufb312',
            'params': '39dede2b392477ee89de845c38c954f92b37539dabec87704c1c32a6c33f5b5c',
            'yaxis': 0,
        },
        'timeseries': [
            {'ts': 1612972680, 'value': 69},
            {'ts': 1612972740, 'value': 70},
        ]
    },
    {
        'name': 'line_id_2',
        'labels': {
            'chart_id': 'wieufb312',
            'params': '39dede2b392477ee89de845c38c954f92b37539dabec87704c1c32a6c33f5b5c',
            'yaxis': 0,
        },
        'timeseries': [
            {'ts': 1612972680, 'value': 50},
            {'ts': 1612972740, 'value': 55},
        ]
    },
]


def test_hash_chart_params():
    hash_1 = hash_dict({'param1': 1, 'param2': 2})
    hash_2 = hash_dict({'param2': 2, 'param1': 1})
    hash_3 = hash_dict({'param2': 1, 'param1': 1})
    assert hash_1 == hash_2
    assert hash_1 != hash_3


@pytest.mark.parametrize(
    'test_input,expected', [
        ({}, []),
        (
            {
                'data': {
                    'categories_ms': [1612972680000, 1612972740000],
                    'graphs': [
                        {
                            'data': [69, 70],
                            'id': 'line_id_1',
                        },
                        {
                            'data': [50, 55],
                            'id': 'line_id_2',
                        }
                    ]
                }
            },
            solomon_data_format
        ),
        (
            {
                'data': {
                    'graphs': [
                        {
                            'data': [{'x': 1612972680000, 'y': 69}, {'x': 1612972740000, 'y': 70}],
                            'name': 'line_id_1',
                            'title': 'Some Title',
                            'yAxis': 0,
                        },
                        {
                            'data': [{'x': 1612972680000, 'y': 50}, {'x': 1612972740000, 'y': 55}],
                            'name': 'line_id_2',
                            'yAxis': 0,
                        },
                    ]
                }
            },
            solomon_data_format
        ),
        (
            {
                'data': {
                    'graphs': [
                        {
                            'data': [[1612972680000, 69], [1612972740000, 70]],
                            'title': 'line_id_1',
                            'yAxis': 0,
                        },
                        {
                            'data': [[1612972680000, 50], [1612972740000, 55]],
                            'title': 'line_id_2',
                            'yAxis': 0,
                        },
                    ]
                }
            },
            solomon_data_format
        ),
        (
            {
                'data': [
                    {
                        'data': [[1612972680000, 69], [1612972740000, 70]],
                        'name': 'line_id_1',
                    },
                    {
                        'data': [[1612972680000, 50], [1612972740000, 55]],
                        'name': 'line_id_2',
                    },
                ]
            },
            solomon_data_format
        ),
    ]
)
def test_rebuild_chart_data(test_input, expected):
    assert rebuild_chart_data('wieufb312', {'param1': 1, 'param2': 2}, test_input) == expected


def test_time_shift():
    assert rebuild_chart_data(
        'wieufb312', {'param1': 1, 'param2': 2}, {
            'data': {
                'graphs': [
                    {
                        'data': [[1612972680000, 69], [1612972740000, 70]],
                        'title': 'line_id_1',
                        'yAxis': 0,
                    },
                ]
            }
        }, -10800,
    ) == [
        {
            'name': 'line_id_1',
            'labels': {
                'chart_id': 'wieufb312',
                'params': '39dede2b392477ee89de845c38c954f92b37539dabec87704c1c32a6c33f5b5c',
                'yaxis': 0,
            },
            'timeseries': [
                {'ts': 1612961880, 'value': 69},
                {'ts': 1612961940, 'value': 70},
            ]
        },
    ]


def test_none_timestamp():
    assert rebuild_chart_data(
        chart_id='wieufb312',
        chart_params={'param1': 1, 'param2': 2},
        chart_data={
            'data': {
                'graphs': [
                    {
                        'data': [[None, None], [1612972740000, 70]],
                        'title': 'line_id_1',
                        'yAxis': 0,
                    },
                ]
            }
        },
    ) == [
        {
            'name': 'line_id_1',
            'labels': {
                'chart_id': 'wieufb312',
                'params': '39dede2b392477ee89de845c38c954f92b37539dabec87704c1c32a6c33f5b5c',
                'yaxis': 0,
            },
            'timeseries': [
                {'ts': 1612972740, 'value': 70},
            ]
        },
    ]
