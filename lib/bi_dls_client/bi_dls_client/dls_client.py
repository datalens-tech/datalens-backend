from __future__ import annotations

import logging
from typing import List, Optional, Dict, Any, Mapping
from urllib.parse import urljoin

from requests import Session, Response

from bi_api_commons.base_models import RequestContextInfo
from bi_api_commons.headers import INTERNAL_HEADER_PROFILING_STACK
from bi_api_commons.utils import stringify_dl_headers
from bi_app_tools.profiling_base import GenericProfiler
from bi_constants.enums import RLSSubjectType
from bi_core.rls import RLSSubject
from bi_core.utils import get_retriable_requests_session  # todo: move to bi api commons
from .exc import DLSNotAvailable, DLSSubjectNotFound

LOGGER = logging.getLogger(__name__)

ST_DLS_TO_RLS = {
    'user': RLSSubjectType.user,
    # 'all': not in DLS.
}


class DLSClient:
    _session: Session
    _rci: RequestContextInfo

    def __init__(
            self,
            host: str,
            secret_api_key: str,
            rci: RequestContextInfo
    ):
        self._host = host
        self._api_key = secret_api_key
        self._rci = rci
        self._session = get_retriable_requests_session()

        req_id = rci.request_id

        if req_id is not None:
            self._session.headers.update({
                'x-request-id': req_id
            })

        assert rci.user_id is not None

        self._session.headers.update({'X-API-Key': self._api_key})

    @property
    def rci(
            self
    ) -> RequestContextInfo:
        return self._rci

    def _request(
            self,
            method: str,
            url: str,
            json: Any = None,
            params: Any = None,
            headers: Mapping[str, Any] = None
    ) -> Response:
        response = self._session.request(
            method=method,
            url=urljoin(
                urljoin(self._host, '_dls/'),
                url
            ),
            json=json,
            params=params,
            headers={
                **(headers or {}),
                INTERNAL_HEADER_PROFILING_STACK: GenericProfiler.get_current_stages_stack_str(),
            },
            timeout=3,
        )

        if response.status_code >= 500:
            raise DLSNotAvailable()

        return response

    def _get_tenancy_headers(
            self
    ) -> dict[str, str]:
        tenant = self._rci.tenant
        assert tenant is not None
        return stringify_dl_headers(tenant.get_outbound_tenancy_headers())

    def get_subjects_by_names(
            self,
            names: List[str],
            fail_on_error: bool = True,
    ) -> List[RLSSubject]:
        response = self._request(
            method='POST',
            url='batch/render_subjects_by_login/',
            headers=self._get_tenancy_headers(),
            params={'require': fail_on_error},
            json={
                'subjects': names,
            }
        )
        if response.status_code == 404 and response.json()['error'] == 'missing_subjects':
            missing_names = response.json()['missing']
            raise DLSSubjectNotFound('Logins do not exist: {}'.format(', '.join(missing_names)),
                                     details={'missing_names': missing_names})

        data = response.json()

        def make_subject(
                name: str,
                info: dict,
        ) -> RLSSubject:
            s_type, s_id = info['name'].split(':', 1)
            return RLSSubject(
                subject_id=s_id,
                subject_type=ST_DLS_TO_RLS[s_type],
                subject_name=name,
            )

        return [
            make_subject(name, info)
            # Note: the key (`name`) in results should be as requested, and the
            # subjects in values (`info`) might not be unique (e.g. if
            # requested logins are `['user', 'User']`).
            for name, info in data['results'].items()]

    def get_node_info(
            self,
            node_id: str
    ) -> Optional[Dict[str, Any]]:
        response = self._request(
            method='GET',
            url=f'nodes/all/{node_id}',
            headers=self._get_tenancy_headers(),
        )
        if response.status_code == 404:
            try:
                dls_error_message = response.json()['message']
                if dls_error_message.startswith("The specified node was not found"):
                    return None
            except Exception:  # noqa
                LOGGER.info("Could not extract error message from DLS response", exc_info=True)

        response.raise_for_status()
        return response.json()

    def create_owner_only_node(
            self,
            node_id: str,
            owner_id: str
    ) -> None:
        response = self._request(
            method='PUT',
            url=f'nodes/entries/{node_id}',
            headers=self._get_tenancy_headers(),
            json={
                'initialPermissionsMode': 'owner_only',
                'initialOwner': f'user:{owner_id}',
                'scope': 'connection'
            },
        )
        response.raise_for_status()

    def assign_admin_role(
            self,
            entry_id: str,
            user_id: str,
            comment: str,
    ) -> None:
        assert isinstance(entry_id, str)
        assert isinstance(user_id, str)
        assert isinstance(comment, str)
        assert (
            self._rci is not None
            and self._rci.user_id is not None
        ), ".assign_admin_role() requires RCI with user ID"

        payload = {
            'diff': {
                'added': {'acl_adm': [{
                    'subject': f'user:{user_id}',
                    'comment': comment,
                }]},
                'removed': {}
            }
        }

        response = self._request(
            method='PATCH',
            url=f'nodes/all/{entry_id}/permissions',
            headers={
                **self._get_tenancy_headers(),
                'X-User-Id': f'user:{self._rci.user_id}',
            },
            json=payload,
        )
        response.raise_for_status()

    def get_node_permissions(
            self,
            node_id: str
    ) -> Optional[Dict[str, Any]]:
        response = self._request(
            method='GET',
            url=f'nodes/all/{node_id}/permissions',
            headers={
                **self._get_tenancy_headers(),
                'X-User-Id': f'user:{self._rci.user_id}',
            },
        )
        response.raise_for_status()
        return response.json()
