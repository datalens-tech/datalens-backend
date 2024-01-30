from __future__ import annotations

import logging
import multiprocessing
import os
import signal
import socket
import subprocess
import sys
import time
from typing import (
    Dict,
    Optional,
    TypeVar,
)

import attr
import requests
from statcommons.log_config import deconfigure_logging


LOGGER = logging.getLogger()

_RUNNER_TV = TypeVar("_RUNNER_TV")


class ForkPopenHack(subprocess.Popen):
    """Execute `func` in a forked process"""

    def __init__(self, target, **kwargs):  # type: ignore  # TODO: fix
        self._target = target
        super().__init__(args=(), **kwargs)  # type: ignore  # TODO: fix

    def _execute_child(self, *args, **kwargs):  # type: ignore  # TODO: fix
        # Most of the arguments are ignored,
        # unix-only version
        # skipping: errpipe.
        pid = os.fork()
        if not pid:  # child
            return self._execute_child_code()
        # parent
        return self._manage_child(pid)

    def _manage_child(self, pid):  # type: ignore  # TODO: fix
        self.pid = pid

    def _execute_child_code(self):  # type: ignore  # TODO: fix
        try:
            self._target()
        finally:
            # HACK: Prevent cleanup of parent's state using suicide.
            os.kill(os.getpid(), signal.SIGKILL)


class ProcessPopenDuck(multiprocessing.Process):
    """`multiprocessing.Process` with some duck-compatibility with `subprocess.Popen`"""

    _retcode = 0

    def __init__(self, *args, **kwargs):  # type: ignore  # TODO: fix
        super().__init__(*args, **kwargs)
        self.start()

    def wait(self, timeout):  # type: ignore  # TODO: fix
        self.join(timeout)
        return self._retcode

    @property
    def returncode(self):  # type: ignore  # TODO: fix
        if self.is_alive():
            return None
        return self._retcode


# TODO FIX: Communicate from thread and fetch STDOUT/STDERR to logs
@attr.s
class WSGIRunner:
    _module: str = attr.ib()
    _callable: str = attr.ib()
    _ping_path: str = attr.ib()
    _bind_port: Optional[int] = attr.ib(default=None)
    _bind_addr: str = attr.ib(default="127.0.0.1")
    _wait_time: float = attr.ib(default=15.0)
    _poll_time: float = attr.ib(default=0.1)
    _wait_term_time: float = attr.ib(default=5.0)
    _env: Optional[Dict[str, str]] = attr.ib(default=None)

    _proc: subprocess.Popen = attr.ib(init=False, default=None)

    def _debug(self, message, *args):  # type: ignore  # TODO: fix
        sys.stderr.write("{} @ {}: ".format(self.__class__.__name__, os.getpid()))
        sys.stderr.write(message % args)
        sys.stderr.write("\n")
        sys.stderr.flush()

    @property
    def bind_addr(self) -> str:
        return self._bind_addr

    @property
    def bind_port(self) -> int:
        if self._bind_port is not None:
            return self._bind_port
        raise ValueError("Bind port was not provided and was not generated yet")

    def find_free_port(self) -> int:
        LOGGER.debug("Finding an available port...")
        # Binding on random port
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self._bind_addr, 0))
        port = s.getsockname()[1]
        s.close()
        LOGGER.debug("Found port %s", port)
        # And hope that nobody bind on it until Flask will bind
        return port

    # TODO FIX: Logging
    def wait_for_up(self) -> None:
        url = f"http://{self._bind_addr}:{self._bind_port}/{self._ping_path.lstrip('/')}"
        start_time = time.monotonic()
        max_time = start_time + self._wait_time
        last_error = None
        resp = None

        while True:
            if self._proc.returncode is not None:
                # TODO FIX: add output
                raise Exception("WSGI process down")

            next_attempt_time = time.monotonic() + self._poll_time
            try:
                resp = requests.get(url, timeout=self._poll_time)
            except Exception as err:
                last_error = err
            else:
                if resp.ok:
                    LOGGER.debug("WSGI app ok on url: %s", url)
                    break
            LOGGER.debug("Still waiting for WSGI app: err=%r / resp=%r", last_error, resp)
            now = time.monotonic()
            if now > max_time:
                raise Exception(
                    f"Timed out waiting for WSGI to come up at {url}",
                    dict(last_error=last_error, last_resp=resp, last_resp_text=resp.text if resp is not None else None),
                )
            sleep_time = next_attempt_time - now
            if sleep_time > 0.001:
                time.sleep(sleep_time)

    def run(self) -> None:
        if self._bind_port is None:
            self._bind_port = self.find_free_port()
        self._run_subproc()
        self.wait_for_up()

    def _make_uwsgi_params(self):  # type: ignore  # TODO: fix
        return (
            "--die-on-term",
            "--master",
            "--module",
            self._module,
            "--callable",
            self._callable,
            "--http",
            f"{self._bind_addr}:{self._bind_port}",
            "--http-timeout",
            "600",
            "--socket-timeout",
            "600",
            # TODO FIX: Figure out why server does not starts within 15 seconds with this settings
            # '--harakiri', '300',
            # '--harakiri-verbose', 'true',
            # '--workers', '16',
            # '--threads', '10',
            "--workers",
            "3",
            "--threads",
            "3",
        )

    def _run_subproc(self):  # type: ignore  # TODO: fix
        cmd = ["uwsgi"] + list(self._make_uwsgi_params())
        env = {
            **(self._env or {}),
            **os.environ,
        }
        self._proc = subprocess.Popen(cmd, env=env)

    def _run_fork_child_code(self):  # type: ignore  # TODO: fix
        self._debug("child: running uwsgi")
        os.environ.update(self._env)  # type: ignore  # TODO: fix
        import pyuwsgi  # noqa

        cmd = [sys.argv[0]] + list(self._make_uwsgi_params())
        sys.argv = cmd
        self._debug("child: cmd: %r", cmd)
        deconfigure_logging()  # so that postfork configure_logging doesn't fail
        try:
            pyuwsgi.run()
        finally:
            self._debug("child: uwsgi done.")

    def shutdown(self):  # type: ignore  # TODO: fix
        try:
            self._proc.terminate()
            exit_code = self._proc.wait(self._wait_term_time)
            LOGGER.debug("Test fixture worker on port %s was terminated. Exit code: %s", self._bind_port, exit_code)
        except Exception:  # noqa
            LOGGER.exception("Exception during test worker graceful shutdown. Going to kill...")
            self._proc.kill()

    def __enter__(self: _RUNNER_TV) -> _RUNNER_TV:
        self.run()  # type: ignore  # TODO: fix
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
