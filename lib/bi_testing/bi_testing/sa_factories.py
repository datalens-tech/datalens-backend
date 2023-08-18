from __future__ import annotations

import logging
import uuid
from typing import List, Optional, Sequence, Callable

import attr

from bi_cloud_integration.iam_rm_client import IAMRMClient
from bi_cloud_integration.model import AccessBindingAction
from bi_testing.external_systems_helpers.top import ExternalSystemsHelperCloud
from bi_testing.cloud_tokens import AccountCredentials


@attr.s(kw_only=True)
class DisposableYCServiceAccountFactory:
    iam_client: Optional[IAMRMClient] = attr.ib()
    folder_id = attr.ib()
    ext_sys_helpers: ExternalSystemsHelperCloud = attr.ib()
    _dls_creation_hook: Optional[Callable] = attr.ib(default=None)  # avoiding circular dependency

    _created_sa_cred_list: List[AccountCredentials] = attr.ib(factory=list)

    def create(self, role_ids: Sequence[str]) -> AccountCredentials:
        sa_name = f'integration-test-sa-{uuid.uuid4()}'
        logging.info(f'Creating sa {sa_name}')
        sa = self.iam_client.create_svc_acct_sync(self.folder_id, sa_name, "")
        logging.info(f'Created sa {sa_name} with id {sa.id}')
        key = self.iam_client.create_svc_acct_key_sync(sa.id, "")
        key_id = key.svc_acct_key_data.id
        logging.info(f'Created key with id {key_id}')
        if role_ids:
            self.iam_client.modify_folder_access_bindings_for_svc_acct_sync(
                svc_acct_id=sa.id,
                acct_type='serviceAccount',
                folder_id=self.folder_id,
                role_ids=role_ids,
                action=AccessBindingAction.ADD,
            )
            roles = self.iam_client.list_svc_acct_role_ids_on_folder_sync(
                svc_acct_id=sa.id,
                acct_type='serviceAccount',
                folder_id=self.folder_id,
            )
            logging.info(f'Sa {sa.id} has {roles}')
        token = self.ext_sys_helpers.yc_credentials_converter.get_service_account_iam_token(
            service_account_id=sa.id,
            key_id=key_id,
            private_key=key.private_key,
        )

        if self._dls_creation_hook:
            self._dls_creation_hook(sa)

        credentials = AccountCredentials(user_id=sa.id, token=token, user_name=sa_name, is_sa=True)
        self._created_sa_cred_list.append(credentials)

        # workaround for iam strange behavior: new account/token sometimes takes a lot of time
        # to become usable, but calling `auth` here makes it usable
        self.ext_sys_helpers.yc_access_service_client.authenticate_sync(iam_token=credentials.token)

        return credentials

    def dispose(self):
        logging.info('SA teardown:')
        while len(self._created_sa_cred_list) > 0:
            creds = self._created_sa_cred_list.pop()
            sa_id = creds.user_id
            self.iam_client.delete_svc_acct_sync(sa_id)
            logging.info(f'Deleted sa with id={sa_id}')

    def __del__(self):
        # better to call explicitly asap
        self.dispose()
