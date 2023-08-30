"""
...

Note: this is a reworked version of `.drivers.http.escaper`, currently used for
literal_binds, might be used for everything.

Unlike escaper, the quoting output is always a string.

Moreover, it should technically be a bytestring,
but currently that does not seem feasible.
"""

from datetime import date, datetime
from decimal import Decimal
import enum

from sqlalchemy.sql.elements import Null

from .util import compat


class Quoter(object):
    """
    Value quoting and serialization for queries.

    Note: the return value is always a string.

    See also: `.drivers.http.escaper`.

    See also: python-clickhouse-client's  `clickhouse.tools.quote_value`.
    """

    number_types = compat.integer_types
    string_types = compat.string_types

    escape_chars = {
        "\b": r"\b",
        "\f": r"\f",
        "\r": r"\r",
        "\n": r"\n",
        "\t": r"\t",
        "\0": r"\0",
        "\\": "\\\\",
        "'": r"\'",
    }

    def quote_str_inner(self, value):
        # TODO: compare the performance with:
        #     import re
        #     escape_chars_re = re.compile(
        #         '[{}]'.format(
        #             ''.join(
        #                 re.escape(char)
        #                 for char in escape_chars)))
        #     value = self.escape_chars_re.sub(
        #         lambda match, escape_chars=escape_chars: (
        #             escape_chars[match.group(0)]),
        #         value)

        value = ''.join(self.escape_chars.get(char, char) for char in value)
        return value

    def quote_str(self, value):
        value = self.quote_str_inner(value)
        return "'" + value + "'"

    def quote_number(self, item):
        return '%d' % (int(item),)

    def quote_date(self, item):
        return 'toDate(%s)' % (
            self.quote_str(item.strftime('%Y-%m-%d')),)

    def quote_datetime(self, item):
        return 'toDateTime(%s)' % (
            self.quote_str(item.strftime('%Y-%m-%d %H:%M:%S')),)

    def quote_decimal(self, item):
        return str(float(item))

    def quote(self, value):
        if value is None or isinstance(value, Null):
            return 'NULL'

        if isinstance(value, self.number_types):
            return self.quote_number(value)

        if isinstance(value, datetime):
            return self.quote_datetime(value)
        if isinstance(value, date):
            return self.quote_date(value)

        if isinstance(value, (Decimal, float)):
            return self.quote_decimal(value)

        if isinstance(value, self.string_types):
            return self.quote_str(value)

        # XXX: shouldn't `()` be used for tuples?
        if isinstance(value, (list, tuple)):
            return "[" + ", ".join(
                self.quote(subitem)
                for subitem in value
            ) + "]"

        if isinstance(value, enum.Enum):
            return self.quote_str(value.name)

        raise Exception("Unsupported object type: {}".format(type(value)))


QUOTER = Quoter()
