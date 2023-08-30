import os
import logging

import requests


LOGGER = logging.getLogger(__name__)


def make_event_to_juggler_request(
        service, status, description='',
        host=os.environ.get('YENV_TYPE'), tags=None,
) -> dict:
    # See also: `bi_core.notification`
    tags = tags or []
    source = os.environ.get('QLOUD_DISCOVERY_INSTANCE')
    return dict(
        url='http://juggler-push.search.yandex.net/events',
        body_data={
            'source': source,
            'events': [
                {
                    'host': host,
                    'service': service,
                    'status': status,
                    'description': description,
                    'tags': tags,
                }
            ]
        },
    )


def process_juggler_response(response_data):
    try:
        event_status_code = response_data['events'][0]['code']
    except Exception:
        LOGGER.warning('juggler: unexpected response data: %r', response_data)
        return

    if event_status_code != 200:
        LOGGER.warning('juggler: non-ok event status: %r (%r)', event_status_code, response_data)


def send_event_to_juggler(
        service, status, description='',
        host=os.environ.get('YENV_TYPE'), tags=None,
        timeout=5):
    request = make_event_to_juggler_request(
        service=service, status=status, description=description,
        host=host, tags=tags,
    )
    url = request['url']
    body_data = request['body_data']

    try:
        response = requests.post(
            url,
            json=body_data,
            timeout=timeout,
        )
        response_data = response.json()
    except Exception as err:
        LOGGER.warning('juggler: request error: %r', err)
    else:
        process_juggler_response(response_data)
