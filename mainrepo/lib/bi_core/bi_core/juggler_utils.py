from __future__ import annotations

from datetime import datetime
from enum import Enum


class JugglerStatus(Enum):
    OK = 0
    WARN = 1
    CRIT = 2


MAX_MSG_LEN = 200


def prepare_msg(msg, max_len=MAX_MSG_LEN):  # type: ignore  # TODO: fix
    msg = msg.replace("\n", "    ")
    if len(msg) > max_len:
        return msg[: max_len - 3] + "..."
    else:
        return msg


def generate_juggler_resp(service: str, status: JugglerStatus, msg):  # type: ignore  # TODO: fix
    ts = datetime.now().isoformat()
    resp = "PASSIVE-CHECK:{svc};{status};{ts} {msg}".format(
        svc=service, status=status.name, ts=ts, msg=prepare_msg(msg)
    )
    return resp


def run_env_subprocess_check(command, timeout):  # type: ignore  # TODO: fix
    import json
    import os
    import subprocess

    env = os.environ.copy()
    env_filename = "/etc/container_environment.json"
    if os.path.exists(env_filename):
        with open(env_filename) as fo:
            env_extra = json.load(fo)
        env.update(env_extra)

    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise

    if proc.returncode:
        msg = "Non-zero exit status: {ret!r}; outputs:{stdout} {stderr}".format(  # type: ignore  # TODO: fix
            ret=proc.returncode, stdout=stdout, stderr=stderr
        )
        raise Exception(msg)
    else:
        resp = str(stdout, "utf-8").rstrip("\n")

    return resp
