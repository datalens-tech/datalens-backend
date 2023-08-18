from __future__ import annotations

import asyncio
import json
import datetime

from statcommons.juggler import send_event_to_juggler

from yadls.settings import settings


__all__ = (
    'periodic_staff_import',
)


def send_task_ok(name, start_ts=None, result=None):
    desc = json.dumps(
        dict(
            end_ts=datetime.datetime.now().isoformat(),
            start_ts=start_ts.isoformat() if start_ts is not None else None,
            result=result
        ),
        default=repr,
        sort_keys=True,
    )
    # Involved environment variables: `QLOUD_DISCOVERY_INSTANCE`, `YENV_TYPE`
    return send_event_to_juggler(
        service=f'dl-dls-{name}',
        status='OK',
        description=desc,
        host=settings.ENV_NAME,
    )


def periodic_staff_import(**kwargs):
    start_ts = datetime.datetime.now()
    from .staff_import import Worker
    worker = Worker()
    result = worker.main(from_cached=False)
    send_task_ok('staff-import', start_ts=start_ts, result=result)


def periodic_cloud_iam_import(**kwargs):
    start_ts = datetime.datetime.now()
    from .cloud_manager import CloudManagerSync
    mgr = CloudManagerSync()
    result = mgr.run_sync_import()
    send_task_ok('iam-import', start_ts=start_ts, result=result)


def periodic_cloud_iam_import_plug(**kwargs):
    """ no-op plug to disable synchronization """
    start_ts = datetime.datetime.now()
    result = dict(WARNING="disabled in code")
    send_task_ok('iam-import', start_ts=start_ts, result=result)


def reject_old_grant_requests(**kwargs):
    start_ts = datetime.datetime.now()
    ttl_days = settings.GRANT_REQUEST_TTL_DAYS
    from .task_reject_old_grants import reject_old_grant_requests_async
    result = asyncio.run(reject_old_grant_requests_async(ttl_days=ttl_days))
    send_task_ok('reject-old-grant-requests', start_ts=start_ts, result=result)


def main():
    """ Run the tasks manager """
    from .runlib import init_logging
    init_logging(app_name='dlstasks')

    from . import db
    from .settings import settings
    from .tasks_manager import TasksManager

    tasks = []
    task_functions = {}

    if settings.STAFF_TOKEN:
        tasks += [dict(name='periodic_staff_import', frequency=60 * 60 * 6, lock_expire=150, lock_renew=30)]
        task_functions['periodic_staff_import'] = periodic_staff_import

    tasks += [dict(name='periodic_cloud_iam_import', frequency=60 * 30, lock_expire=150, lock_renew=30)]
    task_functions['periodic_cloud_iam_import'] = periodic_cloud_iam_import_plug

    tasks += [dict(name='reject_old_grant_requests', frequency=86400, lock_expire=3600, lock_renew=60)]
    task_functions['reject_old_grant_requests'] = reject_old_grant_requests

    mgr = TasksManager(
        task_functions=task_functions, db_engine=db.get_engine(),
        ensured_tasks=tasks)
    return mgr.run()


if __name__ == '__main__':
    main()
