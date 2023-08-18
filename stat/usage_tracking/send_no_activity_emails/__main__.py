"""
Send no-activity emails to inactive users
Used as a sandbox resource
Resources dir: /sandbox/projects/datalens/resources
"""

import argparse
import json
import logging
import time
import os
from datetime import timedelta
from functools import partial
from multiprocessing.pool import ThreadPool
from typing import Dict, List, Iterable, Optional, Tuple

import requests

from yt.wrapper import schema, TypedJob, YtClient, yt_dataclass


DEFAULT_TEMPLATE = 'datalens.user.no-activity'
NOTIFY_SEND_URL = 'https://notify.cloud.yandex.net/v1/send'
SERVICE_ACCOUNT_ERROR_CODE = 'USER_SETTINGS_4XX_ERROR'

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def get_args() -> argparse.Namespace:
    """ Parses command line arguments """

    parser = argparse.ArgumentParser(description='Send emails to inactive users and update users table')
    parser.add_argument('--users-table', required=True, help='inactive users table')
    parser.add_argument('--email-template', default=DEFAULT_TEMPLATE, help='notify template for an email')
    parser.add_argument('--threshold', type=lambda x: timedelta(days=int(x)), default='30',
                        help='an amount of days which user should be inactive in order to send him an email')
    parser.add_argument('--threads-count', type=int, default=8,
                        help='an amount of threads that will be used to send emails')

    return parser.parse_args()


@yt_dataclass
class ActivityRow:
    user_id: str
    last_event_time: schema.Int64
    received_email: bool


@yt_dataclass
class UserId:
    user_id: str


@yt_dataclass
class GetInactiveUsers(TypedJob):
    now: int
    threshold: timedelta

    def __call__(self, row: ActivityRow) -> Iterable[UserId]:
        if not row.received_email and timedelta(seconds=self.now - row.last_event_time) > self.threshold:
            yield UserId(user_id=row.user_id)


@yt_dataclass
class UpdateActivityTable(TypedJob):
    value: bool

    def __call__(self, rows: schema.RowIterator[schema.Variant[ActivityRow, UserId]]) -> Iterable[ActivityRow]:
        row: Optional[ActivityRow] = None
        has_new_value = False

        for input_row, context in rows.with_context():
            if context.get_table_index() == 0:
                row = input_row
            else:
                has_new_value = True

        assert row is not None
        if has_new_value:
            row.received_email = self.value
        yield row


def get_inactive_users_ids(args: argparse.Namespace, yt_client: YtClient) -> List[UserId]:
    with yt_client.TempTable() as tmp:
        yt_client.run_map(
            GetInactiveUsers(now=int(time.time()), threshold=args.threshold),
            source_table=args.users_table,
            destination_table=tmp,
        )
        return list(yt_client.read_table_structured(tmp, UserId))


def send_emails(args: argparse.Namespace, user_ids: List[UserId]) -> Dict[str, dict]:
    def _processor(user_id: UserId, template: str) -> Tuple[str, dict]:
        user_id = user_id.user_id
        try:
            r = requests.post(NOTIFY_SEND_URL, json={'type': template, 'userId': user_id, 'data': {}})
            request_id, trace_id = r.headers.get('x-request-id'), r.headers.get('x-trace-id')
            ids_string = f'Request id: {request_id}, trace id: {trace_id}.'

            if r.status_code != 202:  # HTTP/1.1 202 Accepted (because sending an email is hard)
                LOGGER.warning(f'Failed request for {user_id}, status code: {r.status_code}. {ids_string}')
                try:
                    error_body = r.json()
                    if error_body.get('code') == SERVICE_ACCOUNT_ERROR_CODE:
                        # we've tried to send an email to a service account,
                        # just mark it as sent; see BI-4455
                        return user_id, {}
                    return user_id, error_body
                except requests.exceptions.JSONDecodeError:
                    return user_id, {'Error': r.text}

            LOGGER.info(f'Successful request for {user_id}. {ids_string}')
            return user_id, {}
        except Exception as e:
            return user_id, {'Caught an exception': str(e)}

    exceptions = {}
    processor = partial(_processor, template=args.email_template)
    with ThreadPool(args.threads_count) as pool:
        for user_id, error in pool.imap_unordered(processor, user_ids):
            if error:
                exceptions[user_id] = error

    return exceptions


def update_users_table(args: argparse.Namespace, yt_client: YtClient, user_ids: List[UserId], value: bool) -> None:
    with yt_client.TempTable() as tmp:
        yt_client.write_table_structured(tmp, UserId, user_ids)
        yt_client.run_sort(
            source_table=tmp,
            destination_table=tmp,
            sort_by=['user_id'],
        )
        yt_client.run_sort(
            source_table=args.users_table,
            destination_table=args.users_table,
            sort_by=['user_id'],
        )
        yt_client.run_reduce(
            UpdateActivityTable(value=value),
            source_table=[args.users_table, tmp],
            destination_table=args.users_table,
            reduce_by=['user_id'],
        )


if __name__ == '__main__':
    args = get_args()
    yt_client = YtClient(proxy='hahn')

    # https://st.yandex-team.ru/YT-17234
    transaction = os.environ.get('YT_TRANSACTION')
    if transaction is not None:
        yt_client.COMMAND_PARAMS['transaction_id'] = transaction

    user_ids = get_inactive_users_ids(args, yt_client)
    # first, let's mark all inactive users as if they've received an email;
    # this way if nirvana restarts our process during the send_emails execution we won't send duplicated emails
    update_users_table(args, yt_client, user_ids=user_ids, value=True)

    exceptions = send_emails(args, user_ids)
    # now if we failed to send an email to some user, we will retract the completion mark from them
    if exceptions:
        update_users_table(args, yt_client, user_ids=[UserId(user_id=user_id) for user_id in exceptions], value=False)
        raise RuntimeError(f'Failed to send some emails: {json.dumps(exceptions, indent=4)}')
