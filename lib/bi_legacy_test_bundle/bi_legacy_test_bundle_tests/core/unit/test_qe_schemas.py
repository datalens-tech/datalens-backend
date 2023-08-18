from __future__ import annotations

import uuid
from datetime import datetime

import pytest
from multidict import CIMultiDict

from bi_constants.api_constants import DLContextKey, DLHeadersCommon
from bi_api_commons.base_models import RequestContextInfo, TenantCommon, NoAuthData
from bi_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from bi_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from bi_core.connection_executors.qe_serializer.schemas_common import DBAdapterScopedRCISchema, DBAdapterQueryStrSchema


# Full RCI is used in test-cases to prevent extra work when scope of DBAdapterScopedRCI will be extended
_TEST_REQ_CTX_INFO = (
    DBAdapterScopedRCI.from_full_rci(RequestContextInfo.create(
        request_id=None,
        tenant=None,
        user_id=None,
        user_name=None,
        x_dl_debug_mode=None,
        endpoint_code=None,
        x_dl_context=None,
        plain_headers=None,
        secret_headers=None,
        auth_data=None,
    )),
    DBAdapterScopedRCI.from_full_rci(RequestContextInfo.create(
        request_id=str(uuid.uuid4()),
        tenant=TenantCommon(),
        user_id=str(uuid.uuid4()),
        user_name="vasya",
        x_dl_debug_mode=False,
        endpoint_code="some_endpoint",
        x_dl_context={},
        plain_headers={
            'uber-trace-id': "805324dfd85a80dc699a45d557ccf4a:418f6626fe7843af:c699a45d557ccf4a:1",
        },
        secret_headers={},
        auth_data=None,
    )),
    DBAdapterScopedRCI.from_full_rci(RequestContextInfo.create(
        request_id=str(uuid.uuid4()),
        tenant=TenantCommon(),
        user_id=str(uuid.uuid4()),
        user_name="vasya",
        x_dl_debug_mode=True,
        endpoint_code="some_endpoint",
        x_dl_context={DLContextKey.CHART_ID.value: str(uuid.uuid4())},
        plain_headers={'a': 'b', DLHeadersCommon.FORWARDED_FOR.value: "1.1.1.1,2.2.2.2,3.3.3.3"},
        secret_headers={'c': 'd'},
        auth_data=NoAuthData(),
    )),
    DBAdapterScopedRCI.from_full_rci(RequestContextInfo.create(
        request_id=str(uuid.uuid4()),
        tenant=TenantCommon(),
        user_id=str(uuid.uuid4()),
        user_name="vasya",
        x_dl_debug_mode=True,
        endpoint_code="some_endpoint",
        x_dl_context={k.value: str(uuid.uuid4()) for k in DLContextKey},
        plain_headers={'a': 'b'},
        secret_headers={'c': 'd'},
        auth_data=NoAuthData(),
    )),
    DBAdapterScopedRCI.from_full_rci(RequestContextInfo.create(
        request_id=str(uuid.uuid4()),
        tenant=TenantCommon(),
        user_id=str(uuid.uuid4()),
        user_name="vasya",
        x_dl_debug_mode=True,
        endpoint_code="some_endpoint",
        x_dl_context={'asdf': 'qwer', 'None': None},
        plain_headers={'a': 'b'},
        secret_headers={'c': 'd'},
        auth_data=NoAuthData(),
    )),
    DBAdapterScopedRCI.from_full_rci(RequestContextInfo.create(
        request_id=str(uuid.uuid4()),
        tenant=TenantCommon(),
        user_id=str(uuid.uuid4()),
        user_name="vasya",
        x_dl_debug_mode=True,
        endpoint_code="some_endpoint",
        x_dl_context={DLContextKey.CHART_ID.value: str(uuid.uuid4())},
        plain_headers=CIMultiDict([('a', 'a1'), ('a', 'a2')]),
        secret_headers=None,
        auth_data=None,
    )),
    DBAdapterScopedRCI.from_full_rci(RequestContextInfo.create(
        request_id=str(uuid.uuid4()),
        tenant=TenantCommon(),
        user_id=str(uuid.uuid4()),
        user_name="vasya",
        x_dl_debug_mode=True,
        endpoint_code="some_endpoint",
        x_dl_context={DLContextKey.CHART_ID.value: str(uuid.uuid4()), 'other': None},
        plain_headers=CIMultiDict([('a', 'a1'), ('a', 'a2')]),
        secret_headers=None,
        auth_data=None,
    )),
)


_TEST_DB_ADAPTER_QUERY = (
    DBAdapterQuery(
        query='',
    ),
    DBAdapterQuery(
        query='',
        connector_specific_params={'from': '2020-01-01T00:00:00Z'},
    ),
    DBAdapterQuery(
        query='',
        connector_specific_params={'from': datetime(2020, 1, 1)},
    ),
)


@pytest.mark.parametrize("req_ctx_info", _TEST_REQ_CTX_INFO, )
def test_req_ctx_info_schema(req_ctx_info: DBAdapterScopedRCI):
    schema = DBAdapterScopedRCISchema()
    dumped_req_ctx_info = schema.dump(req_ctx_info)
    reloaded_req_ctx_info = schema.load(dumped_req_ctx_info)

    assert reloaded_req_ctx_info == req_ctx_info


@pytest.mark.parametrize("db_adapter_query", _TEST_DB_ADAPTER_QUERY, )
def test_db_adapter_query_schema(db_adapter_query: DBAdapterQuery):
    schema = DBAdapterQueryStrSchema()
    dumped_db_adapter_query = schema.dump(db_adapter_query)
    reloaded_db_adapter_query = schema.load(dumped_db_adapter_query)

    assert reloaded_db_adapter_query == db_adapter_query
