"""
A very simple HTTP server for pinging a celery instance by running a task and
expecting a result.

Includes:

  * timeout
  * queue length check
  * autoconfiguration from django
  * unistat

py2.7+ & py3.5+ supported.

Using a threaded server to run unistat in parallel with the ping.
"""


from __future__ import division, absolute_import, print_function, unicode_literals

import os
import sys
import time
import socket
import json
import traceback
import logging
from threading import Thread

try:
    import celery
    import celery.exceptions
except ImportError:
    celery = None


if sys.version_info >= (3,):
    from http.server import BaseHTTPRequestHandler, HTTPServer
    PY3 = True
else:
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler  # pylint: disable=import-error
    PY3 = False

# pylint: disable=wrong-import-position
from ..celery import get_queue_size_passive
from ..lock import LockWrap


LOGGER = logging.getLogger(__name__)


# https://stackoverflow.com/a/30312766
# There's also `http.server.ThreadingHTTPServer` in python3.7+
class ThreadedHTTPServer(HTTPServer):

    def process_request(self, request, client_address):
        thread = Thread(
            target=self.__new_request,
            args=(self.RequestHandlerClass, request, client_address, self))
        thread.start()

    def __new_request(self, handler_cls, request, address, server):
        handler_cls(request, address, server)
        self.shutdown_request(request)


# Note: in py2, `BaseHTTPServer.BaseHTTPRequestHandler` is an old-style class.
class RequestHandlerBase(BaseHTTPRequestHandler):

    def __init__(self, *args, **kwargs):
        BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    handler_name = 'celery_ping'

    @staticmethod
    def run_check():
        raise NotImplementedError

    @staticmethod
    def run_dbg_sleep():
        duration = 7
        time.sleep(duration)
        return 200, dict(duration=duration)

    @staticmethod
    def run_unistat():
        raise NotImplementedError

    def route(self, path):
        if path.startswith('/unistat'):
            return self.run_unistat
        if path.startswith('/dbg_sleep/'):
            return self.run_dbg_sleep
        return self.run_check

    # pylint: disable=invalid-name
    def do_GET(self):
        path = self.path
        LOGGER.info(
            ('%s: now=%r, client=%r, '
             'method=%r, path=%r'),
            self.handler_name, self.log_date_time_string(), self.address_string(),
            self.command, path)

        func = self.route(path)
        try:
            status, data = func()
        except Exception as exc:  # pylint: disable=broad-except
            exc_info = sys.exc_info()
            status = 500
            data = dict(
                exc_name=exc.__class__.__name__,
                exc=repr(exc),
                traceback=traceback.format_tb(exc_info[2]),
            )

        # ...
        data_s = json.dumps(data, default=repr)
        if not isinstance(data_s, bytes):
            data_s = data_s.encode('utf-8')
        self.send_response(status)
        self.send_header(b'Content-Type', b'application/json')
        self.end_headers()

        self.wfile.write(data_s)
        self.wfile.write(b'\n')


class HTTPServer6(HTTPServer):

    address_family = socket.AF_INET6


# # This does not work in py2, probably due to the base being an old-style class.
# class ThreadedHTTPServer6(ThreadedHTTPServer, HTTPServer6):
# # Therefore, a bit more copypastey.
class ThreadedHTTPServer6(ThreadedHTTPServer):
    """ ... """

    address_family = socket.AF_INET6


if celery is not None:
    # pylint: disable=too-many-instance-attributes
    class CeleryPingWorkerBase(object):

        # pylint: disable=too-many-arguments
        def __init__(
                self, task_spec, bind='', ipv6=True, port=8081, task_timeout=3.0,
                max_queue_size=5, queue=None, force_ok_flag_file='/tmp/flag__celery_ping_force_ok'):

            dbg_stuff = locals()
            dbg_stuff['_dsm'] = os.environ.get('DJANGO_SETTINGS_MODULE')
            LOGGER.warning('celery_ping worker init: %r', dbg_stuff)

            if queue is None:
                # XXXX: should probably avoid using FQDN as the queue name.
                hostname = socket.getfqdn()
                queue = 'host__{}'.format(hostname)

            self.bind = bind
            self.port = port
            self.ipv6 = ipv6
            self.httpd_cls = ThreadedHTTPServer6 if self.ipv6 else ThreadedHTTPServer

            self.task_spec_orig = task_spec
            self.task_spec = dict(self.task_spec_orig, queue=queue)
            self.task_timeout = task_timeout
            self.max_queue_size = max_queue_size
            self.force_ok_flag_file = force_ok_flag_file
            self.httpd = None
            self.celery_app = None
            self.handler_cls = None

            self.lock_cm = LockWrap(timeout=self.task_timeout * 0.9)

        def ensure_celery_app(self):
            if self.celery_app is not None:
                return self.celery_app
            celery_app = celery.current_app
            assert celery_app
            if celery_app.main == 'default':
                raise Exception('Celery current app is default, refusing to touch it')
            self.celery_app = celery_app
            return celery_app

        def run(self):
            self.ensure_celery_app()
            self.handler_cls = self._make_handler_cls()
            self.httpd = self.httpd_cls((self.bind, self.port), self.handler_cls)
            LOGGER.info("celery_ping: starting the HTTP server on port %r", self.port)
            return self.httpd.serve_forever()

        def _make_handler_cls(self):

            # class RequestHandler(RequestHandlerBaseMixin, BaseHTTPRequestHandler):
            class RequestHandler(RequestHandlerBase):
                run_check = staticmethod(self.run_check)
                run_unistat = staticmethod(self.run_unistat)

            return RequestHandler

        def run_check(self, **kwargs):
            with self.lock_cm:
                return self.run_check_i(**kwargs)

        def run_check_i(self):
            """ () -> http_status (int), data (object) """
            task_spec = self.task_spec
            result = dict(
                _task_spec=task_spec,
            )
            celery_app = self.celery_app

            queue_size = get_queue_size_passive(
                celery_app=celery_app, queue=task_spec['queue'])
            result.update(queue_size=queue_size, max_queue_size=self.max_queue_size)

            if self.force_ok_flag_file and os.path.exists(self.force_ok_flag_file):
                return 200, dict(
                    result=None,
                    warning='force_ok_by_flag_file',
                    flag_file=self.force_ok_flag_file,
                    queue_size=queue_size,
                )

            if queue_size is not None and queue_size > self.max_queue_size:
                return 400, dict(result, error='Queue overflow')

            LOGGER.debug("celery_ping: queue_size=%r", queue_size)

            task_res = celery_app.send_task(**task_spec)
            try:
                result = task_res.get(timeout=self.task_timeout)
            except celery.exceptions.TimeoutError:
                return 400, dict(
                    result, error='No result within timeout', timeout=self.task_timeout)
            finally:
                task_res.forget()
            return 200, dict(result=result)

        def run_unistat(self):
            celery_app = self.celery_app
            from ..unistat.celery import celery_unistat
            results = celery_unistat(celery_app)
            if hasattr(results, 'items'):
                results = list(results.items())
            return 200, results

    class CeleryPingWorkerDjango(CeleryPingWorkerBase):

        def run(self):
            import django

            django_setup = getattr(django, 'setup', None)
            if django_setup:  # pylint: disable=using-constant-test
                django_setup()  # pylint: disable=not-callable
            else:  # django1.5
                from django.db.models import get_apps  # pylint: disable=no-name-in-module
                get_apps()

            from django.conf import settings
            sec_marker = getattr(settings, 'SECFILES_LOADED', None)
            if sec_marker is not None:  # supported
                if not sec_marker:  # not loaded
                    LOGGER.info("celery_ping: secfiles suorted and not loaded, quitting in 2 seconds.")
                    time.sleep(2)
                    return None
            return super(CeleryPingWorkerDjango, self).run()

    class CeleryPingWorkerStatface(CeleryPingWorkerDjango):

        def run_check(self):
            """ () -> http_status (int), data (object) """
            if os.environ.get('SFAPI_FORCE_DISABLE'):
                return 200, dict(message='Disabled by SFAPI_FORCE_DISABLE')
            return super(CeleryPingWorkerStatface, self).run_check()


# # Usage example:
# #!/usr/bin/env python
#
# from __future__ import division, absolute_import, print_function, unicode_literals
#
# import os
#
#
# def main():
#     if os.environ.get('APP_DAEMON_NAME') != 'celery':
#         print("celery_ping: no celery workers intended on the current host, sleeping away")
#         import time
#         time.sleep(86400)
#         return
#
#     os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'app_name.settings')
#     from statcommons.applib.celery_ping import CeleryPingWorker
#     worker = CeleryPingWorker(task_spec=dict(name='app_name.celery_app.debug_task'))
#     return worker.run()
#
#
# if __name__ == '__main__':
#     main()
