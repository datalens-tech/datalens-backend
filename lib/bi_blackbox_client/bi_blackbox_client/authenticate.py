import os
from collections import OrderedDict
from urllib.parse import urlencode
from typing import Optional, Dict

import aiohttp
import requests

from bi_api_commons.aiohttp.aiohttp_client import BIAioHTTPClient, PredefinedIntervalsRetrier

from bi_blackbox_client.tvm_client import TVM_INFO, get_tvm_headers
from bi_blackbox_client import exc


OAUTH_SCOPES = ('stat:all',)
NO_AUTH_RESULT = dict(
    user_id=None,
    username=None,
    blackbox_response=None,
)


def _blackbox_req_prepare(  # type: ignore  # TODO: fix
        session_id_cookie=None,
        sessionid2_cookie=None,
        authorization_header=None,
        userip=None,
        host=None,
        tvm_info=None,
        blackbox_client_id_name=None,
        force_tvm=False,
        scopes=OAUTH_SCOPES,
        statbox_id=None,  # kind of request-id for blackbox sessionid method. can be found in bb logs in field `idnt`
        user_ticket=False,
):
    params = OrderedDict((
        ('userip', userip),
        ('host', host),
        ('format', 'json'),
    ))
    body_params = OrderedDict()  # type: ignore  # TODO: fix
    headers = {}  # type: ignore  # TODO: fix

    # TODO CONSIDER: Throw exception on both cookies and Authorization header or no one of parameters
    if session_id_cookie or sessionid2_cookie:
        params.update((
            ('method', 'sessionid'),
        ))
        body_params.update((
            ('sessionid', session_id_cookie),
        ))
        if sessionid2_cookie:
            body_params.update((
                ('sslsessionid', sessionid2_cookie),
            ))

        if statbox_id is not None:
            params.update((
                ('statbox_id', statbox_id),
            ))

        if user_ticket:
            params.update((
                ('get_user_ticket', 'yes'),
            ))
    elif authorization_header:
        params.update((
            ('method', 'oauth'),
            ('scopes', ','.join(scopes))
        ))
        headers.update((('Authorization', authorization_header),))
        # Note: empty body, POST request, still works.
    else:
        return None, None, None

    if tvm_info is None and (
            os.environ.get('BI_FORCE_BLACKBOX_TVM') or
            force_tvm):
        tvm_info = TVM_INFO

    if tvm_info is not None:
        # TODO: use `bi_blackbox_client.tvm`
        tvm_headers = get_tvm_headers(tvm_info=tvm_info, blackbox_client_id_name=blackbox_client_id_name)
        headers.update(tvm_headers)

    # body='sessionid=…&sslsessionid=…`,
    # no content-type,
    # works,
    # not properly documented.
    body = urlencode(body_params)

    return params, headers, body


def _blackbox_resp_parse(resp_data):  # type: ignore  # TODO: fix
    if resp_data['error'] == 'OK':
        return dict(
            username=resp_data['login'],
            user_id=resp_data['uid']['value'],
            blackbox_response=resp_data,
        )

    return dict(NO_AUTH_RESULT, blackbox_response=resp_data)


def authenticate(  # type: ignore  # TODO: fix
        session_id_cookie=None,
        sessionid2_cookie=None,
        authorization_header=None,
        userip=None,
        host=None,
        requests_session=requests.Session(),
        blackbox_uri='https://blackbox.yandex-team.ru/blackbox',
        tvm_info=None,
        blackbox_client_id_name=None,
        force_tvm=None,
        scopes=OAUTH_SCOPES,
        timeout=5.0,
        statbox_id=None,
        user_ticket=False,
):

    params, headers, body = _blackbox_req_prepare(
        session_id_cookie=session_id_cookie,
        sessionid2_cookie=sessionid2_cookie,
        authorization_header=authorization_header,
        userip=userip,
        host=host,
        tvm_info=tvm_info,
        blackbox_client_id_name=blackbox_client_id_name,
        force_tvm=force_tvm,
        scopes=scopes,
        statbox_id=statbox_id,
        user_ticket=user_ticket,
    )
    if params is None and headers is None:
        return NO_AUTH_RESULT

    resp = requests_session.post(
        blackbox_uri,
        params=params,
        data=body,
        timeout=timeout,
        verify=True,
        headers=headers,
    )
    resp.raise_for_status()
    resp_data = resp.json()

    return _blackbox_resp_parse(resp_data)


async def authenticate_async(  # type: ignore  # TODO: fix
        session_id_cookie=None,
        sessionid2_cookie=None,
        authorization_header=None,
        userip=None,
        host=None,
        aiohttp_client_session: Optional[aiohttp.ClientSession] = None,
        blackbox_uri='https://blackbox.yandex-team.ru/blackbox',
        tvm_info=None,
        blackbox_client_id_name=None,
        force_tvm=None,
        scopes=OAUTH_SCOPES,
        timeout=1.0,
        statbox_id=None,
        user_ticket=False,
):
    params, headers, body = _blackbox_req_prepare(
        session_id_cookie=session_id_cookie,
        sessionid2_cookie=sessionid2_cookie,
        authorization_header=authorization_header,
        userip=userip,
        host=host,
        tvm_info=tvm_info,
        blackbox_client_id_name=blackbox_client_id_name,
        force_tvm=force_tvm,
        scopes=scopes,
        statbox_id=statbox_id,
        user_ticket=user_ticket,
    )

    if params is None and headers is None:
        raise exc.InsufficientAuthData()

    # TODO FIX: Handle params=None/headers=None

    async with BIAioHTTPClient(
        base_url=blackbox_uri,
        conn_timeout_sec=1, read_timeout_sec=timeout,
        raise_for_status=True,
        retrier=PredefinedIntervalsRetrier(retry_intervals=(0.2, 0.3, 0.5), retry_methods={'POST'}),
        session=aiohttp_client_session,
    ) as bb_http_client:
        async with bb_http_client.request(
            method='POST',
            params={p_name: p_val for p_name, p_val in params.items() if p_val is not None},
            data=body,
            headers=headers,
            conn_timeout_sec=1,
        ) as resp:
            resp_data = await resp.json()

    return _blackbox_resp_parse(resp_data)


def get_user_ticket_header(
    session_id: Optional[str],
    sessionid2: Optional[str],
    userip: Optional[str] = None,
    host: Optional[str] = None,
) -> Dict:
    blackbox_resp = authenticate(
        session_id_cookie=session_id,
        sessionid2_cookie=sessionid2,
        user_ticket=True,
        force_tvm=True,
        userip=userip,
        host=host,
        scopes=(),
    )
    if blackbox_resp['blackbox_response'] is None or blackbox_resp['blackbox_response'].get('user_ticket') is None:
        return {}
    user_ticket = blackbox_resp['blackbox_response']['user_ticket']
    return {
        'X-Ya-User-Ticket': user_ticket,
    }
