from __future__ import annotations

import abc
import requests
from typing import Dict

import attr

from bi_constants.enums import NotificationStatus


@attr.s(frozen=True)
class Notification:
    source: str = attr.ib()
    object: str = attr.ib()
    status: NotificationStatus = attr.ib()
    description: str = attr.ib()


class BaseNotifier(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def send_notification(  # type: ignore  # TODO: fix
        self,
        notification: Notification,
    ):
        pass


class JugglerNotifier(BaseNotifier):
    def __init__(  # type: ignore  # TODO: fix
        self,
        juggler_host: str,
        **ignore
    ):
        self.host = juggler_host

    def send_notification(self, notification: Notification):  # type: ignore  # TODO: fix
        # see also: `statcommons.juggler`
        payload = self._build_payload(notification)
        self._send_event_to_juggler(payload)

    def _send_event_to_juggler(self, payload: Dict):  # type: ignore  # TODO: fix
        response = requests.post(
            f'{self.host}/events',
            json=payload,
        )
        response.raise_for_status()

    def _build_payload(self, notification: Notification) -> Dict:
        payload = {
            'source': 'datalens',
            'events': [
                {
                    'host': notification.source,
                    'service': notification.object,
                    'status': notification.status.value,
                    'description': notification.description,
                }
            ]
        }
        return payload


class NoneNotifier(BaseNotifier):
    def __init__(self, **ignore):  # type: ignore  # TODO: fix
        pass

    def send_notification(self, notification: Notification):  # type: ignore  # TODO: fix
        pass


class NotifierFactory:
    def __init__(self):  # type: ignore  # TODO: fix
        self._notifiers = {
            'juggler': JugglerNotifier,
            'none': NoneNotifier,
        }

    def get_notifier(
        self,
        notifier_type: str,
        params: Dict,
    ) -> BaseNotifier:
        if notifier_type not in self._notifiers:
            raise ValueError('Invalid notifier type.')
        return self._notifiers[notifier_type](**params)
