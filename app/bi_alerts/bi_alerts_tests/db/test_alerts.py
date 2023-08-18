from __future__ import annotations

import json
import pytest


DEFAULT_PAYLOAD = {
    "chart_id": "hfigj7niect0g",
    "oauth": "AQAD-token-here",
    "chart_params": {},
    "chart_lines": [
        {
            "type": "single",
            "yaxis": 0,
            "name": "Users"
        }
    ],
    "alert": {
        "name": "Users",
        "description": "Мониторинг количества пользователей",
        "type": "threshold",
        "window": 3600,
        "aggregation": "AVG",
        "params": {
            "alarm": [
                {
                    "condition": "GT",
                    "value": 10000
                }
            ]
        }
    },
    "notifications": [
        {
            "recipient": {
                "email": "seray@yandex-team.ru"
            },
            "transport": "email"
        },
        {
            "recipient": {
                "email": "yares@yandex-team.ru"
            },
            "transport": "email"
        },
    ],
    "time_shift": -10800,
}


@pytest.mark.asyncio
async def test_create_alert(web_app, db_models, mock_solomon_api, mock_blackbox_api):
    resp = await web_app.post(
        '/alerts/v1/alert',
        data=json.dumps(DEFAULT_PAYLOAD)
    )
    assert resp.status == 200
    alert_id = await resp.json()
    assert 'id' in alert_id.keys()


@pytest.mark.asyncio
async def test_get_alert(web_app, db_models, mock_solomon_api, mock_blackbox_api):
    resp = await web_app.post(
        '/alerts/v1/alert',
        data=json.dumps(DEFAULT_PAYLOAD)
    )
    assert resp.status == 200
    alert_id = await resp.json()
    resp = await web_app.get(
        '/alerts/v1/alert/{}'.format(alert_id['id'])
    )
    assert resp.status == 200
    alert = await resp.json()
    assert len(DEFAULT_PAYLOAD['notifications']) == len(alert['notifications'])
    assert DEFAULT_PAYLOAD['time_shift'] == alert['time_shift']


@pytest.mark.asyncio
async def test_delete_alert(web_app, db_models, mock_solomon_api, mock_blackbox_api):
    resp = await web_app.post(
        '/alerts/v1/alert',
        data=json.dumps(DEFAULT_PAYLOAD)
    )
    assert resp.status == 200
    alert_id = await resp.json()
    resp = await web_app.delete(
        '/alerts/v1/alert/{}'.format(alert_id['id'])
    )
    assert resp.status == 200


@pytest.mark.asyncio
async def test_update_alert(web_app, db_models, mock_solomon_api, mock_blackbox_api):
    resp = await web_app.post(
        '/alerts/v1/alert',
        data=json.dumps(DEFAULT_PAYLOAD)
    )
    assert resp.status == 200
    alert_id = await resp.json()
    resp = await web_app.get(
        '/alerts/v1/alert/{}'.format(alert_id['id'])
    )
    assert resp.status == 200
    alert = await resp.json()
    assert alert['alert']['description'] == 'Мониторинг количества пользователей'
    assert alert['time_shift'] == DEFAULT_PAYLOAD['time_shift']
    DEFAULT_PAYLOAD['alert'].update(
        description='Другое описание',
    )
    DEFAULT_PAYLOAD['time_shift'] = 10800
    DEFAULT_PAYLOAD['notifications'] = DEFAULT_PAYLOAD['notifications'][:-1]
    resp = await web_app.put(
        '/alerts/v1/alert/{}'.format(alert_id['id']),
        data=json.dumps(DEFAULT_PAYLOAD)
    )
    assert resp.status == 200
    resp = await web_app.get(
        '/alerts/v1/alert/{}'.format(alert_id['id'])
    )
    assert resp.status == 200
    alert = await resp.json()
    assert alert['alert']['description'] == 'Другое описание'
    assert len(alert['notifications']) == 1
    assert alert['time_shift'] == 10800


@pytest.mark.asyncio
async def test_alerts_list(web_app, db_models, mock_solomon_api, mock_blackbox_api):
    nums = 3
    for _ in range(nums):
        await web_app.post(
            '/alerts/v1/alert',
            data=json.dumps(DEFAULT_PAYLOAD)
        )
    resp = await web_app.post(
        '/alerts/v1/list',
        data=json.dumps({
            'chart_id': DEFAULT_PAYLOAD['chart_id']
        })
    )
    assert resp.status == 200
    data = await resp.json()
    assert len(data['alerts']) == nums
    assert len(DEFAULT_PAYLOAD['notifications']) == len(data['alerts'][0]['notifications'])


@pytest.mark.asyncio
async def test_alerts_check(web_app, db_models, mock_solomon_api, mock_blackbox_api):
    resp = await web_app.post(
        '/alerts/v1/check',
        data=json.dumps({
            'chart_id': DEFAULT_PAYLOAD['chart_id']
        })
    )
    assert resp.status == 200
    data = await resp.json()
    assert data['has_alerts'] is False
    await web_app.post(
        '/alerts/v1/alert',
        data=json.dumps(DEFAULT_PAYLOAD)
    )
    resp = await web_app.post(
        '/alerts/v1/check',
        data=json.dumps({
            'chart_id': DEFAULT_PAYLOAD['chart_id']
        })
    )
    assert resp.status == 200
    data = await resp.json()
    assert data['has_alerts'] is True
