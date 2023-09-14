# coding: utf-8
"""
Modifications of `ylog`.
"""

from collections import OrderedDict
import datetime
import json
import logging
import logging.handlers
import traceback
from typing import (
    Any,
    Callable,
)

# Minor note:
# `context` in the formatter is a wrong approach.
# Use a 'logging filter' that adds whatever context you need to the record
# attibutes (essentially, an annotator).


# Common usable formats for file-based logs.
FILE_FORMAT = "%(asctime)s %(name)-15s %(levelname)-10s %(message)s"
DT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
# Remains here for compatibility
SYSLOG_FORMAT = "%(asctime)s %(name)-15s %(levelname)-10s %(message)s"


def smart_str(value):
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def robust_smart_str(value, default="???"):
    try:
        return smart_str(value)
    except Exception:
        return default


def exception_str(value):
    """
    Formats Exception object into a string. Unlike default str():

    - can handle unicode strings in exception arguments
    - tries to format arguments as str(), not as repr()
    """
    try:
        return ", ".join([smart_str(item) for item in value])
    except (TypeError, AttributeError):  # happens for non-iterable values
        try:
            return smart_str(value)
        except UnicodeEncodeError:
            try:
                return repr(value)
            except Exception:
                return "<Unprintable value>"


def format_post(request, post_truncate_size=1024):
    """
    Formats request post data into a string. Depending on content type it's
    either a dict or raw post data.
    """
    if request.method == "POST" and (
        request.META.get("CONTENT_TYPE", "").split(";")[0]
        not in ["application/x-www-form-urlencoded", "multipart/form-data"]
    ):
        value = request.read()
        if len(value) > post_truncate_size:
            value = value[:post_truncate_size] + b"..."
        return value
    # else:
    return str(request.POST)


def format_frame_locals(frame, max_length=512, infix="...", prefix_tpl="      %s: ", suffix_tpl=" // %d bytes"):
    ret = ["    Locals:\n"]
    for key, value in frame.f_locals.items():
        result = repr(value)
        prefix = prefix_tpl % (key,)
        if len(result) > max_length:
            suffix = suffix_tpl % (len(result),)
            length = 80 - len(prefix) - len(infix) - len(suffix)
            result = result[: (length + 1) // 2] + infix + result[-(length // 2) :] + suffix
        ret.append("%s%s\n" % (prefix, result))
    return ret


def format_locals(tb):
    ret = []
    while tb is not None:
        ret.append(format_frame_locals(tb.tb_frame))
        tb = tb.tb_next
    return ret


def format_exception_with_locals(exc_type, exc_value, tb):
    try:
        exception = traceback.format_exception_only(exc_type, exc_value)
        locations = traceback.format_tb(tb)
        local_vars = format_locals(tb)
        formatted = ["Traceback (most recent call last):\n"]
        for loc, var in zip(locations, local_vars):
            formatted.append(loc)
            formatted.extend(var)
        formatted.extend(exception)
        return formatted
    except Exception:
        formatted = ["Couldn't print detailed traceback.\n"] + traceback.format_exception(exc_type, exc_value, tb)
        return formatted


def get_record_exc_repr(record, fmt=FILE_FORMAT, datefmt=DT_FORMAT, full=False, show_locals=False):
    """
    Formats exception log events into a short useful one-line message containing
    the exception class, module name, line number and the exception value. Exception
    info triple should be passed as a record parameter record.exc_info.

    When used with Django the parameter `full=True` adds extended data to the log message:
    request path, GET and POST representations and full traceback. The original log
    message is dropped (it's usually just the string "Internal server error").

    @param show_locals: if True, then every traceback frame would contain information
                        about local variables
    @param value_length_limit: the length, to which the variable value would be
                               stripped
    @param wrap_width: width for textwrap.wrap
    """

    # construct required fields in record.__dict__
    record.message = record.getMessage()
    if fmt.find("%(asctime)") >= 0:
        record.asctime = datetime.datetime.fromtimestamp(record.created).strftime(datefmt)

    lines = [fmt % record.__dict__]

    if full:
        req = getattr(record, "request", None)
        if req is not None:
            lines.append("Path: %s" % req.path)
            lines.append("GET: %s" % req.GET)
            lines.append("POST: %s" % format_post(req))
        exc_info = record.exc_info
        if exc_info is not None:
            if show_locals:
                exc_lines = format_exception_with_locals(*exc_info)
            else:
                exc_lines = traceback.format_exception(*exc_info)
            lines.append("".join(exc_lines))
    elif record.exc_info is not None and record.exc_info[-1] is not None:  # and not full:
        exception, value, tb = record.exc_info
        # we do not use traceback module because it doesn't provide
        # the way to determine module_name

        # find innermost call
        inner = tb
        while inner.tb_next:
            inner = inner.tb_next

        lineno = inner.tb_lineno
        f_globals = inner.tb_frame.f_globals
        module_name = f_globals.get("__name__", "<unknown>")

        record.message = "%-20s %s:%s %s" % (exception.__name__, module_name, lineno, exception_str(value))
        lines[0] = fmt % record.__dict__

    return "\n".join([smart_str(line) for line in lines])


class RecordDataFormatterMixin:
    unspecified = object()

    default_skipped_record_attrs = (
        # 'name',  # 'root',
        "msg",  # 'Example logged error: %r',
        "args",  # (ZeroDivisionError('division by zero'),),
        # 'levelname',  # 'ERROR',
        "levelno",  # 40,
        "pathname",  # 'tmpf.py',
        "filename",  # 'tmpf.py',
        "module",  # 'tmpf',
        # # covered by the func-based attr.
        # 'exc_info',  # (ZeroDivisionError, ZeroDivisionError('division by zero'), <traceback at 0x7f21bdc5b7c8>),
        "exc_text",  # None,
        "stack_info",  # None,
        # 'lineno',  # 11,
        # 'funcName',  # '<module>',
        "created",  # 1575299079.958725,
        "msecs",  # 958.7249755859375,
        "relativeCreated",  # 16066.834449768066,
        "thread",  # 139783057811200,
        "threadName",  # 'MainThread',
        "processName",  # 'MainProcess',
        "process",  # 409783}
        # func-based:
        # 'timestamp',
        # 'timestampns',
        # 'isotimestamp',
        # 'message',
        # 'pid',
        # 'exc_type',
        # 'exc',
        # compat, just-in-case, remove-later:
        "log_context",
        "dlog_context",
    )

    func_based_record_attrs = (
        # `self.get_record_...(record)`
        # Some of them are simple renames ('timestamp', 'pid')
        "timestamp",
        "timestampns",
        "isotimestamp",
        "message",
        "pid",
        "exc_type",
        "exc_info",
    )

    @staticmethod
    def get_record_timestamp(record):
        return record.created

    @staticmethod
    def get_record_timestampns(record, ns=int(1e9)):
        return int(record.created * ns)

    @staticmethod
    def get_record_isotimestamp(record, fmt=DT_FORMAT):
        dt = datetime.datetime.fromtimestamp(record.created)
        return dt.strftime(fmt)

    # Maybe: get_record_tzaware_isotiemstamp

    @staticmethod
    def get_record_message(record):
        return record.getMessage()

    @staticmethod
    def get_record_pid(record):
        return record.process

    @staticmethod
    def get_record_exc_type(record):
        if not record.exc_info:
            return None
        if not isinstance(record.exc_info, tuple):
            return None
        return record.exc_info[0].__name__

    @staticmethod
    def get_record_exc_info(record):
        exc_info = record.exc_info
        if exc_info is None:
            return None
        if not isinstance(exc_info, tuple):
            return exc_info

        # return record.formatException()

        _, exc, _ = exc_info
        exc_info_formatted = "".join(smart_str(val) for val in traceback.format_exception(*exc_info))

        # str(exc) at the beginning for some readability.
        return "{}\n{}".format(robust_smart_str(exc), exc_info_formatted)

    def __init__(self, skipped_record_attrs=unspecified):
        if skipped_record_attrs is self.unspecified:
            skipped_record_attrs = self.default_skipped_record_attrs
        self.skipped_record_attrs = frozenset(skipped_record_attrs)

    def record_to_data(self, record):
        """Make items dict from the record"""
        skiplist = self.skipped_record_attrs
        result = {}

        result.update({key: val for key, val in vars(record).items() if key not in skiplist})

        # Dubious: Prioritize the func-based over the base.
        for name in self.func_based_record_attrs:
            if name not in skiplist:
                func = getattr(self, "get_record_{}".format(name))
                result[name] = func(record)

        return result


class JsonExtFormatter(RecordDataFormatterMixin, logging.Formatter):
    def format(self, record):
        log_data = self.record_to_data(record)
        return self.dumps(log_data)

    @staticmethod
    def dumps(data):
        return json.dumps(data, default=robust_smart_str)


class JsonEventFormatter(JsonExtFormatter):
    """
    Use case:

        the_event = {'type': 1, 'value': 2}
        logger.info('An event', extra=dict(event=the_event))

    """

    def record_to_data(self, record):
        event = getattr(record, "event", None)
        if event is None or not isinstance(event, dict):
            return {"_unknown_record_event": event}
        return event


class TaggedSysLogHandlerBase(logging.handlers.SysLogHandler):
    """
    A version of SysLogHandler that adds a tag to the logged line
    so that it can be sorted by the syslog daemon into files.

    Generally equivalent to using a formatter but semantically more
    similar to FileHandler's `filename` parameter.
    """

    def __init__(self, *args, **kwargs):
        self.syslog_tag = kwargs.pop("syslog_tag")
        super(TaggedSysLogHandlerBase, self).__init__(*args, **kwargs)

    def format(self, *args, **kwargs):  # pylint: disable=arguments-differ
        res = super(TaggedSysLogHandlerBase, self).format(*args, **kwargs)
        return self.syslog_tag + " " + res


class TaggedSysLogHandler(TaggedSysLogHandlerBase):
    """An addition that sets the SO_SNDBUF to a large value to allow large log lines."""

    _sndbuf_size = 5 * 2**20

    def __init__(self, *args, **kwargs):
        self._sbdbuf_size = kwargs.pop("sbdbuf_size", self._sndbuf_size)
        super(TaggedSysLogHandler, self).__init__(*args, **kwargs)
        self.configure_socket(self.socket)

    def configure_socket(self, sock):
        import socket

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self._sndbuf_size)


class LogRecordCaptureHandler(logging.Handler):
    """
    Similar to pytest's LogCaptureHandler,
    but only collects the record objects,
    without formatting them.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def reset(self) -> None:
        self.records = []


class RecordMutators:
    """
    A mutator manager:
    wrap the logging factory
    and apply a the mutators on the newly created record.

    For performance, the log record mutation is used, instead of log record mappers.
    """

    def __init__(self):
        self.mutators = OrderedDict()
        self.old_factory = None

    def apply(self, require=True):
        """
        Wrap the current log record factory.
        Should only be done once.
        """
        if self.old_factory is not None:
            if require:
                raise Exception("This mutators-wrapper was already `apply`ed.")
            return
        self.old_factory = logging.getLogRecordFactory()
        logging.setLogRecordFactory(self)

    def add_mutator(self, name: str, mutator: Callable[[Any], None], force=False):
        """
        Register a mutator.
        `name` is required to ensure idempotency.
        """
        # ... would be nice to somehow validate the mutator callable here.
        if not force:
            current = self.mutators.get(name)
            if current is not None and current is not mutator:
                raise ValueError("Attempting to replace an existing mutator", name, current, mutator)
        self.mutators[name] = mutator

    def __call__(self, *args, **kwargs):
        """Log record factrory entry point"""
        if self.old_factory is None:
            raise Exception("This mutators-wrapper was not `apply`ed yet.")
        record = self.old_factory(*args, **kwargs)
        for mutator in self.mutators.values():
            mutator(record)
        return record


def make_attr_setter(key: str, value: Any, validate=False):
    """
    Make a mutator that does a single `setattr` with a constant.
    """

    def attr_setter(record):
        if validate:
            if hasattr(record, key):
                raise Exception("Record already has the attribute", key)
        setattr(record, key, value)

    return attr_setter


def test_logmutators():
    capture = LogRecordCaptureHandler()
    logger = logging.getLogger("test_logmutators")
    logger.propagate = False
    logger.addHandler(capture)
    logger.setLevel(logging.INFO)

    def set_1_1(record):
        record.test_key_1 = "test_val_1"

    def set_2_2(record):
        record.test_key_2 = "test_val_2"

    LOGMUTATORS.add_mutator("set_1", set_1_1)
    LOGMUTATORS.add_mutator("set_2", set_2_2)

    other_mutators = RecordMutators()

    def set_1_not_1(record):
        record.test_key_1 = "test_val_not_1"

    def set_3_3(record):
        record.test_key_3 = "test_val_3"

    other_mutators.add_mutator("set_1", set_1_not_1)
    other_mutators.add_mutator("set_3", set_3_3)

    logger.info("test_01")
    record = capture.records[-1]
    assert record.msg == "test_01"
    assert not hasattr(record, "test_key_1")
    assert not hasattr(record, "test_key_2")
    assert not hasattr(record, "test_key_3")

    LOGMUTATORS.apply()
    logger.info("test_02")
    record = capture.records[-1]
    assert record.msg == "test_02"
    assert record.test_key_1 == "test_val_1"
    assert record.test_key_2 == "test_val_2"
    assert not hasattr(record, "test_key_3")

    other_mutators.apply()
    logger.info("test_03")
    record = capture.records[-1]
    assert record.msg == "test_03"
    assert record.test_key_1 == "test_val_not_1"
    assert record.test_key_2 == "test_val_2"
    assert record.test_key_3 == "test_val_3"


# The recommended mutators-wrapper singleton.
LOGMUTATORS = RecordMutators()
