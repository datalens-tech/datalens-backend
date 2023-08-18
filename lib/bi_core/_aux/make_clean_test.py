#!/usr/bin/env python3
"""
Get an all-oauth token here:
https://oauth.yandex-team.ru/authorize?response_type=token&client_id=658b2255f8e24be5a60fe32ed908c2f7


This script runs a sandbox task,

the task is based on a prepared scheduler that runs daily tests:
https://sandbox.yandex-team.ru/scheduler/19027/view

the task takes and runs scripts at
https://a.yandex-team.ru/arc/trunk/arcadia/datalens/backend/clean_tests_runner

the entry script (`over_ssh.sh`), for now, provisions a QYP host (from a image prepared by `prepare.sh`)

and runs the tests-run script (`main.sh`).

the tests-run script clones the specified repo and branch, and runs `make test`.
"""

from __future__ import annotations

import getpass
import json
import logging
import os
import shlex
import subprocess
import urllib

import hjson
import requests


class Worker:

    # https://sandbox.yandex-team.ru/scheduler/19027/view
    ref_scheduler_id = 19027

    sandbox_url = 'https://sandbox.yandex-team.ru'

    logger = logging.getLogger('make_clean_test')

    _token = None

    def __init__(self):
        self.reqr = requests.Session()

    @staticmethod
    def get_token(fln='~/.release.hjson'):
        with open(os.path.expanduser(fln)) as fobj:
            data = hjson.load(fobj)
        return data['oauth_token']

    @property
    def token(self):
        if self._token is None:
            self._token = self.get_token()
        return self._token

    def _log(self, message, *args, **kwargs):
        level = kwargs.pop('level', logging.INFO)
        self.logger.log(level, message, *args, **kwargs)

    def req_sb(self, method, uri, **kwargs):
        kwargs.setdefault('timeout', 60)

        url = urllib.parse.urljoin(self.sandbox_url, uri)

        headers = dict(kwargs.pop('headers', None) or {})
        headers.update({
            'Authorization': 'OAuth {}'.format(self.token),
        })
        resp = self.reqr.request(method, url, headers=headers, **kwargs)
        if not resp.ok:
            self._log('Non-ok sandbox response: %r', resp.text, level=logging.ERROR)
        resp.raise_for_status()
        return resp

    def run_task(self, cfg, **kwargs):
        self._log('Going to create task: %r', cfg)

        create_resp = self.req_sb(
            'POST', '/api/v1.0/task',
            json=cfg, **kwargs)
        resp_data = create_resp.json()
        task_id = resp_data['id']
        if resp_data['status'] != 'DRAFT':
            raise Exception("Unexpected new-task status", resp_data)

        self._log('Task id: %r', task_id)
        start_resp = self.req_sb(
            'PUT', '/api/v1.0/batch/tasks/start',
            json=[task_id],
            **kwargs)
        resp_data = start_resp.json()
        assert isinstance(resp_data, list)
        start_result = resp_data[0]
        start_status = start_result.get('status') or ''

        if start_status.lower() == 'warning':
            self._log('Warning while starting task %r: %r', task_id, start_result, level=logging.WARNING)
        elif not start_status:
            raise Exception(
                'Failed to start task {!r}: {!r}'.format(
                    task_id, start_result))

        return task_id

    def run_sh(self, *full_command, as_bytes=False, unstripped=False):
        self._log("Running command: %r", full_command)
        process = subprocess.Popen(
            full_command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            # stderr=None,
            shell=False,
        )
        stdout, _ = process.communicate()
        ret = process.returncode
        self._log("Return code: %r", ret)
        if ret != 0:
            raise Exception("Non-zero exit", dict(ret=ret, cmd=full_command))
        # shell logic: using `"$(some_cmd)"` also strips the newline.
        if not unstripped and stdout and stdout[-1:] == b'\n':
            stdout = stdout[:-1]
        if not as_bytes:
            stdout = stdout.decode('utf-8')
        return stdout

    def cmd(self, cmd, **kwargs):
        full_command = shlex.split(cmd)
        return self.run_sh(*full_command, **kwargs)

    _command_tpl = (
        "REPO_URL={repo_path}\n"
        "REPO_BRANCH={repo_branch}\n"
        "DELETE_AFTERWARDS={delete_afterwards}\n"
        "\n"
        "export REPO_URL REPO_BRANCH DELETE_AFTERWARDS\n"
        "exec ./over_ssh.sh provision\n"
    )

    def get_task_cfg(self, repo_branch, repo_path, username=None):
        repo_name = repo_path.rsplit('/', 1)[-1].rsplit('.', 1)[0]

        command_params_base = dict(
            repo_path=repo_path,
            repo_branch=repo_branch,
            delete_afterwards='1',
        )
        command_params_sh = {
            key: shlex.quote(str(value))
            for key, value in command_params_base.items()}

        sched_resp = self.req_sb(
            'GET', '/api/v1.0/scheduler/{}'.format(self.ref_scheduler_id))
        sched_cfg = sched_resp.json()
        cfg = sched_cfg['task']

        def proc_field(fcfg):
            name = fcfg['name']
            if name == 'command':
                fcfg.update(
                    value=self._command_tpl.format(**command_params_sh),
                )
            return fcfg

        cfg.update(
            description=f'datalens tests: {repo_name} {repo_branch}\nauthor: {username}',
            tags=['DATALENS', 'DATALENS-BACKEND-TEST',
                  repo_name.upper(), repo_branch.upper(),
                  'MAKE-CLEAN-TEST'],
            # owner='STATFACE',
            custom_fields=[proc_field(fcfg) for fcfg in cfg['custom_fields']],
            status='DRAFT',
        )

        # (?) cfg['custom_fields'] -> cfg['input_parameters']

        cfg.pop('last', None)
        if username:
            for notify_cfg in cfg.get('notifications') or []:
                notify_cfg.setdefault('recipients', []).append(username)

        return cfg

    def main(self):
        try:
            import pyaux.runlib
            pyaux.runlib.init_logging(level=1)
        except Exception:
            logging.basicConfig(level=1)

        branch_info_s = self.cmd('arc branch --json --verbose --verbose')
        branch_info = json.loads(branch_info_s)
        branches_current = [item for item in branch_info if item.get('current')]
        if len(branches_current) != 1:
            raise Exception("Could not select a single current branch",
                            dict(branches_current=branches_current))
        branch_current = branches_current[0]
        repo_branch = branch_current['remote']

        path = os.path.realpath('.')
        path_pieces = path.split('/datalens/backend/')
        if len(path_pieces) != 2 or not path_pieces[-1]:
            raise Exception("Unknown repo path (should contain '/datalens/backend/' in the middle")
        repo_path = 'arc:/datalens/backend/{}'.format(path_pieces[-1])

        username = getpass.getuser()
        if username == 'sandbox':
            username = None

        cfg = self.get_task_cfg(
            repo_branch=repo_branch, repo_path=repo_path,
            username=username,
        )

        task_id = self.run_task(cfg)

        task_url = '{}/task/{}/view'.format(
            self.sandbox_url, task_id)
        print(task_url)


if __name__ == '__main__':
    Worker().main()
