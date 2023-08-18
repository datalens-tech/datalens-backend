import aiohttp
import json
import logging
from typing import List, Optional, Dict, Any
from hashlib import md5
from urllib.parse import urljoin

import shortuuid

from bi_alerts.settings import from_granular_settings
from bi_alerts.enums import AlertType

from bi_core.tvm import TvmCliSingleton, TvmDestination, get_tvm_headers


LOGGER = logging.getLogger(__name__)

AGGREGATION_MAP = {
    'LAST_NON_NAN': 'last',
    'AVG': 'avg',
    'MIN': 'min',
    'MAX': 'max',
}
CONDITION_MAP = {
    'EQ': '==',
    'NE': '!=',
    'GTE': '>=',
    'GT': '>',
    'LT': '<',
    'LTE': '<=',
}
ABS_RELATIVE_PROGRAM = '''
    let data = {{{selectors}}};
    let shifted_data = shift(data, {param_period});

    let results = abs(shifted_data - data) / data;
    let status = any_of(map(results, x -> abs({aggregation}(x)) {condition} {param_pvalue}));

    alarm_if(status);
'''


def hash_str(string: str) -> str:
    return md5(string.encode()).hexdigest()


class SolomonChannel:
    settings = from_granular_settings()
    method_mapper = {
        'email': 'datalensEmail',
    }

    def __init__(self, method: str, recipient: str):
        self.method = self.method_mapper.get(method, method)
        self.recipient = recipient
        self.prefix = self.settings.SOLOMON_PREFIX
        self.name = f'{self.method} {self.recipient}'
        self.id = f'{self.prefix}_{self.method}_{hash_str(self.recipient)}'

    def __eq__(self, other):  # type: ignore  # TODO: fix
        return isinstance(other, SolomonChannel) and self.method == other.method \
            and self.recipient == other.recipient

    def __hash__(self):  # type: ignore  # TODO: fix
        return hash(f'{self.id}')

    def as_dict(self):  # type: ignore  # TODO: fix
        return {
            'id': self.id,
            'method': {
                self.method: {
                    'recipients': [self.recipient, ],
                },
            },
            'name': self.name,
        }


class SolomonAlert:
    settings = from_granular_settings()

    def __init__(
        self,
        name: str,
        description: str,
        window: int,
        aggregation: str,
        alert_type: AlertType,
        alert_params: dict,
        selectors: dict,
        annotations: Optional[dict] = None,
        id: Optional[str] = None,
    ):
        self.name = name
        self.description = description
        self.window = window
        self.aggregation = aggregation
        self.alert_type = alert_type
        self.alert_params = alert_params
        self.selectors = self._build_selectors_line(**selectors)
        self.channels = set()  # type: ignore  # TODO: fix
        self.prefix = self.settings.SOLOMON_PREFIX
        self.annotations = annotations if annotations else dict()
        if id is not None:
            self.id = id
        else:
            self.id = f'{self.prefix}_{selectors["chart_id"]}_{shortuuid.uuid()}'

    def as_dict(self):  # type: ignore  # TODO: fix
        return {
            'id': self.id,
            'type': self._build_alert_type_payload(),
            'name': self.name,
            'windowSecs': self.window,
            'delaySecs': 0,
            'description': self.description,
            'channels': [
                {
                    'id': channel.id,
                    'config': {
                        'notifyAboutStatuses': ['ALARM', 'ERROR'],
                        'repeatDelaySecs': 0,
                    },
                } for channel in self.channels
            ],
            'annotations': self.annotations,
            'resolvedEmptyPolicy': 'RESOLVED_EMPTY_DEFAULT',
            'noPointsPolicy': 'NO_POINTS_DEFAULT',
        }

    def add_channel(self, channel: SolomonChannel):  # type: ignore  # TODO: fix
        self.channels.add(channel)

    def _build_selectors_line(  # type: ignore  # TODO: fix
        self,
        chart_id: str, hashed_params: str, shard_id: str, lines: List[dict],
    ):
        if not lines:
            raise IndexError('There is no lines to build')
        if lines[0]['type'] == 'all':
            name = '*'
        else:
            name = '|'.join([line['name'] for line in lines])
        yaxis = lines[0]['yaxis']

        return (
            f"project='{self.settings.SOLOMON_PROJECT}', cluster='{self.settings.SOLOMON_CLUSTER}', "
            f"service='{shard_id}', host='alerts', chart_id='{chart_id}', params='{hashed_params}', "
            f"yaxis='{yaxis}', name='{name}'"
        )

    def _build_alert_type_payload(self):  # type: ignore  # TODO: fix
        if self.alert_type == AlertType.threshold:
            return {
                'threshold': {
                    'predicateRules': [
                        {
                            'thresholdType': self.aggregation,
                            'comparison': condition['condition'].upper(),
                            'threshold': condition['value'],
                            'targetStatus': status.upper(),
                        } for status, conditions in self.alert_params.items() for condition in conditions
                    ],
                    'selectors': self.selectors,
                }
            }
        elif self.alert_type == AlertType.absrelative:
            # only single alarm yet
            alarm_params = self.alert_params['alarm'][0]
            return {
                'expression': {
                    'program': ABS_RELATIVE_PROGRAM.format(
                        selectors=self.selectors,
                        param_period=alarm_params['period'],
                        param_pvalue=alarm_params['pvalue'],
                        aggregation=AGGREGATION_MAP[self.aggregation],
                        condition=CONDITION_MAP[alarm_params['condition']],
                    ).strip(),
                }
            }
        raise NotImplementedError('Alert type is not implemented')


class TvmCliSingletonSolomon(TvmCliSingleton):
    destinations = (TvmDestination.SolomonPre, TvmDestination.SolomonProd,)  # type: ignore  # TODO: fix


class TvmCliSingletonSolomonFetcher(TvmCliSingleton):
    destinations = (TvmDestination.SolomonFetcherPre, TvmDestination.SolomonFetcherProd,)  # type: ignore  # TODO: fix
    enable_service_ticket_checking = True


class SolomonClient:

    _logger = LOGGER

    def __init__(
        self,
        url: str,
        token: str,
        project: str,
        prefix: str,
        connect_timeout: int = 5,
        read_timeout: int = 30,
        tvm_id: Optional[TvmDestination] = None,
    ):
        self._url = url
        self._project = project
        self._prefix = prefix
        self._token = token
        self._tvm_id = tvm_id
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(
                sock_connect=connect_timeout,
                sock_read=read_timeout,
            )
        )

    async def _build_headers(self) -> Dict[str, Any]:
        default_headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': 'application/json',
            'User-Agent': f'{aiohttp.http.SERVER_SOFTWARE} DataLens Alerts',
        }
        if self._tvm_id is not None:
            tvm_cli = await TvmCliSingletonSolomon.get_tvm_cli()
            auth_headers = get_tvm_headers(
                tvm_cli=tvm_cli, destination=self._tvm_id,
            )
        else:
            auth_headers = {'Authorization': f'OAuth {self._token}'}
        return {**default_headers, **auth_headers}

    async def create_channel(self, data: SolomonChannel):  # type: ignore  # TODO: fix
        async with self._session.post(
            urljoin(self._url, f'projects/{self._project}/notificationChannels'),
            data=json.dumps(data.as_dict()),
            headers=await self._build_headers(),
        ) as resp:
            resp.raise_for_status()
            self._logger.info(f'Solomon notification channel created {data.id}')

    async def create_channel_if_not_exists(self, data: SolomonChannel):  # type: ignore  # TODO: fix
        async with self._session.get(
            urljoin(
                self._url,
                f'projects/{self._project}/notificationChannels/{data.id}'
            ),
            headers=await self._build_headers(),
        ) as resp:
            status = resp.status
        if status == 404:
            await self.create_channel(data)

    async def create_alert(self, data: SolomonAlert):  # type: ignore  # TODO: fix
        async with self._session.post(
            urljoin(self._url, f'projects/{self._project}/alerts'),
            data=json.dumps(data.as_dict()),
            headers=await self._build_headers(),
        ) as resp:
            resp.raise_for_status()
            self._logger.info(f'Solomon alert created: {data.id}')

    async def get_alert(self, alert_id: str):  # type: ignore  # TODO: fix
        async with self._session.get(
            urljoin(self._url, f'projects/{self._project}/alerts/{alert_id}'),
            headers=await self._build_headers(),
        ) as resp:
            resp.raise_for_status()
            response = await resp.json()
        return response

    async def delete_alert(self, alert_id: str):  # type: ignore  # TODO: fix
        async with self._session.delete(
            urljoin(self._url, f'projects/{self._project}/alerts/{alert_id}'),
            headers=await self._build_headers(),
        ) as resp:
            resp.raise_for_status()
            self._logger.info(f'Solomon alert deleted: {alert_id}')

    async def update_alert(self, alert_id: str, data: SolomonAlert, version: int):  # type: ignore  # TODO: fix
        payload = data.as_dict()
        payload['version'] = version
        async with self._session.put(
            urljoin(self._url, f'projects/{self._project}/alerts/{alert_id}'),
            data=json.dumps(payload),
            headers=await self._build_headers(),
        ) as resp:
            resp.raise_for_status()
            self._logger.info(f'Solomon alert updated: {alert_id}')
