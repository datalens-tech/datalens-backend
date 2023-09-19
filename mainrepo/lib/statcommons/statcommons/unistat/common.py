"""
Common tools for unistat-format stats collection
"""

from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

from json import dumps as json_dumps
import os
import socket


__all__ = (
    "get_sys_memstatus",
    "get_context_data",
    "get_common_prefix",
    "maybe_float_size",
)


FQDN = None


def get_fqdn():
    global FQDN
    if FQDN is not None:
        return FQDN
    FQDN = socket.getfqdn()
    return FQDN


# http://man7.org/linux/man-pages/man5/proc.5.html
STATM_COLS = "size resident shared text lib data dt".split()


def get_sys_memstatus(pid):
    if not pid:
        return {}
    try:
        with open("/proc/{}/statm".format(pid), "r") as fobj:
            data_statm = fobj.read()
    except (OSError, IOError) as exc:
        return dict(_exc=repr(exc))
    data_m = data_statm.strip("\n").split(" ")
    return dict(zip(STATM_COLS, data_m))


CONTEXT_ENV_VARS = dict(
    project="QLOUD_PROJECT",
    application="QLOUD_APPLICATION",
    environment="QLOUD_ENVIRONMENT",
    # component='QLOUD_COMPONENT',
    geo="QLOUD_DATACENTER",
    instance="QLOUD_INSTANCE",
)


def get_context_data(require=True):
    env_vars = CONTEXT_ENV_VARS
    context_data = {key: (os.environ.get(src) or "").lower() for key, src in env_vars.items()}
    if require and not all(context_data.values()):
        raise Exception("Not all context environment variables are set: %r -> %r" % (env_vars, context_data))
    return context_data


def get_common_prefix(require=True):
    context_data = get_context_data(require=require)
    if not all(context_data.values()):
        return ""
    prefix = (
        "prj={project}.{application}.{environment};"
        "geo={geo};"
        # Currently does nothing, alas. Use filters like `tier={name}-*` instead:
        # 'component={component};'
        "tier={instance};"
    ).format(**context_data)
    return prefix


def maybe_float_size(value, divider):
    """
    RTFS.

    Convenience shorthand.
    """
    if value is None:
        return None
    return float(value) / divider


def results_to_response(results, indent="    "):
    if hasattr(results, "items"):
        results = results.items()
    yield "[\n"
    beginning = True
    for name, value in results:
        if not beginning:
            yield ",\n"
        beginning = False
        yield indent
        yield json_dumps([name, value])
    yield "\n"
    yield "]\n"


def dump_for_prometheus(results):
    for label, value in results:
        yield "{} {}\n".format(label, value)
