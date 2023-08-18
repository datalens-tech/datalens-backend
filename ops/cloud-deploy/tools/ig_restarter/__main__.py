#!/usr/bin/env python3

from __future__ import annotations

import time
import sys
import datetime
import os
from typing import Union, Optional
from urllib.parse import urljoin

import jwt
import requests


class PreprodContext:
    service_account_id = 'bfbdtpfu9qlotm99j4h6'
    key_id = 'bfbbebsm5dbkf58od128'
    iam_url = 'https://iam.api.cloud-preprod.yandex.net/iam/v1/tokens'
    compute_api = 'https://compute.api.cloud-preprod.yandex.net'


class ProdContext:
    service_account_id = 'aje2uruba661cok977h5'
    key_id = 'ajesn9i5qqte1ko29k2v'
    iam_url = 'https://iam.api.cloud.yandex.net/iam/v1/tokens'
    compute_api = 'https://compute.api.cloud.yandex.net'


class IGRestarter:
    def __init__(self, ig_id: str, env_context: Union[type(PreprodContext), type(ProdContext)]):
        self.ig_id = ig_id
        self.env_context = env_context
        self.sa_private_key = self.get_sa_private_key()
        self.iam_token = self.get_iam_token()

    def get_sa_private_key(self):
        return os.environ['SA_PRIVATE_KEY']

    def get_iam_token(self) -> str:
        now = int(time.time())
        payload = {
            'aud': self.env_context.iam_url,
            'iss': self.env_context.service_account_id,
            'iat': now,
            'exp': now + 360,
        }
        encoded_jwt_token = jwt.encode(
            payload,
            self.sa_private_key,
            algorithm='PS256',
            headers={'kid': self.env_context.key_id}
        )

        resp = requests.post(
            self.env_context.iam_url,
            json={'jwt': encoded_jwt_token.decode()}
        )
        resp.raise_for_status()
        return resp.json()['iamToken']

    def _ask_compute(self, http_method: str, uri: str, data: Optional[dict] = None, params: Optional[dict] = None):
        resp = requests.request(
            http_method,
            urljoin(self.env_context.compute_api, uri),
            json=data,
            params=params,
            headers={'Authorization': f'Bearer {self.iam_token}'}
        )
        if resp.status_code == 400:
            print('400:', resp.content)
        resp.raise_for_status()
        return resp.json()

    def restart(self):
        metadata = self._ask_compute(
            'GET', f'compute/v1/instanceGroups/{self.ig_id}',
            params={'view': 'FULL'}
        )['instanceTemplate']['metadata']

        timestamp = str(datetime.datetime.utcnow())
        metadata['restart_timestamp_utc'] = str(timestamp)

        self._ask_compute(
            'PATCH', f'compute/v1/instanceGroups/{self.ig_id}',
            data={
                'updateMask': 'instanceTemplate.metadata',
                'instanceTemplate': {'metadata': metadata},
            }
        )


def main():
    env_context = {
        'preprod': PreprodContext,
        'prod': ProdContext,
    }[sys.argv[1]]

    restarter = IGRestarter(sys.argv[2], env_context)
    restarter.restart()


if __name__ == '__main__':
    main()
