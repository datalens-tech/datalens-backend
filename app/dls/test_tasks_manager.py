#!/usr/bin/env python3
"""
See also:

    python -c 'import pandas as pd; df = pd.read_csv(".tasks.log", delimiter=" ", names=("ts", "dt", "name")); df = df[df["name"] == "test_01"]; df["ts_d"] = df["ts"] - df["ts"].shift(); print(df)'
"""

from __future__ import annotations

import time
import logging
import yadls.tasks_manager as mod
from yadls import db


LOG = logging.getLogger('test_integrated')


def make_test_func(name, sleep_time):

    def test_func(**kwargs):
        print(name, "in;", kwargs)
        with open('.tasks.log', 'a', 1) as fo:
            fo.write("%.3f %s %s\n" % (time.time(), mod.datetime_now().isoformat(), name))
            time.sleep(sleep_time)
        print(name, "out;", kwargs)

    return test_func


def test_02(**kwargs):
    print("test_02 in", kwargs)
    with open('.tasks.log', 'a', 1) as fo:
        fo.write("%.3f %s\n" % (time.time(), "test_02"))
    time.sleep(20)
    print("test_02 out", kwargs)


def test_main():
    logformat = "%(asctime)s: %(levelname)-13s: %(process)s | %(name)s: %(message)s"
    try:
        from pyaux.runlib import init_logging
        init_logging(level=1, format=logformat)
    except Exception:
        logging.basicConfig(level=1, format=logformat)

    sample_tasks = (
        dict(name='test_01', frequency=10, lock_expire=10, lock_renew=2),
        dict(name='test_02', frequency=15, lock_expire=120, lock_renew=5),
        dict(name='test_03', frequency=20, lock_expire=10, lock_renew=9),
    )

    task_functions = dict(
        test_01=make_test_func('test_01', 10),
        test_02=make_test_func('test_02', 10),
        test_03=make_test_func('test_03', 15),
    )

    now = mod.datetime_now()
    try:
        mgr = mod.TasksManager(
            task_functions=task_functions, db_engine=db.get_engine(), ensured_tasks=sample_tasks)
        mgr.run()
        # mgr.prepare()
        # for step in range(10):
        #     sleep_time = mgr._process_once(now=now)
        #     assert sleep_time < 100, sleep_time
        #     time.sleep(1)
        #     now += mod.timedelta(seconds=sleep_time)
    finally:
        tbl = db.PeriodicTask.__table__
        engine = db.get_engine()
        engine.execute(tbl.delete().where(tbl.c.name.in_([task['name'] for task in sample_tasks])))


if __name__ == '__main__':
    test_main()
