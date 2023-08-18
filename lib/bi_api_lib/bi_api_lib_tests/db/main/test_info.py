from __future__ import annotations

import pytest

from bi_constants.enums import AggregationFunction, BIType, ConnectionType

from bi_api_connector.form_config.models.base import ConnectionFormMode

from bi_api_lib.enums import BI_TYPE_AGGREGATIONS
from bi_api_lib.connection_forms.registry import CONN_FORM_FACTORY_BY_TYPE

from bi_connector_yql.core.yq.constants import CONNECTION_TYPE_YQ


def test_get_field_types_info(client):
    resp = client.get('/api/v1/info/field_types')
    assert resp.status_code == 200, resp.json
    resp_data = resp.json

    names = set(item['name'] for item in resp_data['types'])
    expected = set(
        item.name
        for item in BI_TYPE_AGGREGATIONS
        if item not in (BIType.uuid, BIType.markup, BIType.datetimetz, BIType.datetime, BIType.unsupported))
    assert names == expected

    for api_record in resp_data['types']:
        aggs = set(AggregationFunction[x] for x in api_record['aggregations'])
        expected = set(BI_TYPE_AGGREGATIONS[BIType[api_record['name']]])
        assert aggs == expected


def test_get_connectors(client):
    resp = client.get('/api/v1/info/connectors')
    assert resp.status_code == 200, resp.json
    connector_info = resp.json

    connectors_legacy = connector_info['result']
    sections = connector_info['sections']

    assert all(section['title'] is not None for section in sections)
    assert all('includes' not in connector for connector in connectors_legacy)

    assert set(conn_info['conn_type'] for conn_info in connectors_legacy if conn_info['conn_type'] != '')
    assert all(isinstance(conn_info['alias'], str) for conn_info in connectors_legacy)
    assert all(
        conn_info['backend_driven_form'] ==
        (CONN_FORM_FACTORY_BY_TYPE.get(ConnectionType(conn_info['conn_type'] or ConnectionType.unknown.name)) is not None)
        for conn_info in connectors_legacy
    )


@pytest.mark.parametrize('mode_name', [
    mode.name for mode in ConnectionFormMode
])
@pytest.mark.parametrize('conn_type_name', [
    conn_type.name for conn_type in ConnectionType
])
def test_get_connector_form(client, conn_type_name, mode_name):
    """ There is not much we can test, other than the fact that all forms can be built successfully """

    form_resp = client.get(f'/api/v1/info/connectors/forms/{conn_type_name}/{mode_name}')
    assert form_resp.status_code == 200
    assert 'form' in form_resp.json


def test_get_connector_form_bad_request(client):
    form_resp = client.get(f'/api/v1/info/connectors/forms/bad_conn_type/{ConnectionFormMode.create.name}')
    assert form_resp.status_code == 400, form_resp.json

    form_resp = client.get(f'/api/v1/info/connectors/forms/{CONNECTION_TYPE_YQ.name}/bad_form_mode')
    assert form_resp.status_code == 400, form_resp.json
