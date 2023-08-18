#!/usr/bin/env python3

from __future__ import annotations

import sys

from bi_app_tools.entry_point import BIEntryPoint


class BITestProjectWorkerEntryPoint(BIEntryPoint):
    do_register_sa_dialects = False

    subcommands_global = BIEntryPoint.subcommands_global + (
        'task-processor-cli',
        'task-processor-worker',
        'task-processor-worker-health-check',
    )

    @staticmethod
    def main_task_processor_worker():
        from bi_test_project_worker.app import run_standalone_worker
        run_standalone_worker()

    @staticmethod
    def main_task_processor_cli():
        from bi_test_project_worker.app import run_cli
        run_cli(sys.argv[1:])

    @staticmethod
    def main_task_processor_worker_health_check():
        from bi_test_project_worker.app import run_health_check
        run_health_check()


ENTRY_POINT = BITestProjectWorkerEntryPoint()


if __name__ == '__main__':
    sys.exit(ENTRY_POINT.main())
