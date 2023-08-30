# coding: utf8
"""
...
"""

from __future__ import unicode_literals


__all__ = (
    'Parts',
    'ParseError',
    'SansIOBase',
)

from ..util.compat import to_str


class Parts(object):

    # # Note: also testing the most basic structures
    # # to make it easier to cythonize if need be.
    # META, DATA, DATACHUNK, TOTALS, STATS, NEED_DATA, FINISHED = range(7)
    # # Debug-oriented:
    META = 'PART_META'
    DATA = 'PART_DATA'
    DATACHUNK = 'PART_DATACHUNK'
    TOTALS = 'PART_TOTALS'
    STATS = 'PART_STATS'
    NEED_DATA = 'NEED_DATA'
    FINISHED = 'FINISHED'


class ParseError(ValueError):

    def __init__(self, error, state='', line='', etcetera=None):
        self.error = to_str(error)
        self.state = to_str(state)
        self.line = line
        self.etcetera = etcetera
        message = 'state: %s; error: %s; line: %r' % (
            self.state, self.error, line)
        if etcetera:
            message += ' (%r)' % (etcetera,)
        super().__init__(message)


class SansIOBase(object):
    """
    Parser interface for async integration.
    See also: `h11.Connection`.
    """

    def receive_data(self, data):
        raise NotImplementedError

    def next_event(self):
        raise NotImplementedError
