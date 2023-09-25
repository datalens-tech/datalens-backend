
import decimal
from ipaddress import (
    IPv4Address, IPv6Address,
    IPv4Network, IPv6Network,
)

import sqlalchemy as sa
from sqlalchemy.sql.type_api import to_instance, UserDefinedType
from sqlalchemy import types, func, and_, or_

from .dt_utils import (
    parse_date, parse_datetime, parse_datetime64, parse_timezone,
)
from .ext.clauses import NestedColumn
from .util.compat import text_type


def _noop(value):
    return value


def get_http_postprocess(coltype):
    """
    A hack to account for the fact that non-parametrized types can often happen
    to be non-instantiated.
    """
    if isinstance(coltype, type):
        coltype = coltype()  # should be parameterless in this case
    return getattr(coltype, '_chsa_http_postprocess', _noop)


class CHTypeMixin(object):
    """ Common base-class marker for the CHSA types """

    def _chsa_http_postprocess(self, value):
        return value


class String(CHTypeMixin, types.String):
    pass


class Int(CHTypeMixin, types.Integer):

    def _chsa_http_postprocess(self, value):
        return int(value)


class Float(CHTypeMixin, types.Float):

    def _chsa_http_postprocess(self, value):
        if value is None:
            # sample non-nullable case: `select 1 / 0 format JSON`
            return None
        return float(value)


class Array(CHTypeMixin, types.TypeEngine):
    __visit_name__ = 'array'

    def __init__(self, item_type):
        self.item_type = item_type
        self.item_type_impl = to_instance(item_type)
        super(Array, self).__init__()

    def literal_processor(self, dialect):
        item_processor = self.item_type_impl.literal_processor(dialect)

        def process(value):
            processed_value = []
            for x in value:
                if item_processor:
                    x = item_processor(x)
                processed_value.append(x)
            return '[' + ', '.join(processed_value) + ']'
        return process


class Nullable(CHTypeMixin, types.TypeEngine):
    __visit_name__ = 'nullable'

    def __init__(self, nested_type):
        self.nested_type = nested_type
        self._nested_http_postprocess = get_http_postprocess(self.nested_type)
        super(Nullable, self).__init__()

    def _chsa_http_postprocess(self, value):
        if value is None:
            return None
        return self._nested_http_postprocess(value)


class UUID(String):
    __visit_name__ = 'uuid'


class LowCardinality(CHTypeMixin, types.TypeEngine):
    __visit_name__ = 'lowcardinality'

    def __init__(self, nested_type):
        self.nested_type = nested_type
        super(LowCardinality, self).__init__()


class Int8(Int):
    __visit_name__ = 'int8'


class UInt8(Int):
    __visit_name__ = 'uint8'


class Int16(Int):
    __visit_name__ = 'int16'


class UInt16(Int):
    __visit_name__ = 'uint16'


class Int32(Int):
    __visit_name__ = 'int32'


class UInt32(Int):
    __visit_name__ = 'uint32'


class Int64(Int):
    __visit_name__ = 'int64'


class UInt64(Int):
    __visit_name__ = 'uint64'


class Float32(Float):
    __visit_name__ = 'float32'


class Float64(Float):
    __visit_name__ = 'float64'


class Bool(CHTypeMixin, types.Boolean):
    __visit_name__ = 'bool'

    def _chsa_http_postprocess(self, value):
        return bool(value)


class Date(CHTypeMixin, types.Date):
    __visit_name__ = 'date'

    def _chsa_http_postprocess(self, value):
        return parse_date(value)


class Date32(CHTypeMixin, types.Date):
    __visit_name__ = 'date32'

    def _chsa_http_postprocess(self, value):
        return parse_date(value)


class DateTime(CHTypeMixin, types.DateTime):
    __visit_name__ = 'datetime'

    def _chsa_http_postprocess(self, value):
        return parse_datetime(value)


class DateTime64(CHTypeMixin, types.DateTime):
    __visit_name__ = 'datetime64'

    def __init__(self, precision):
        super(DateTime64, self).__init__()
        self.precision = precision

    def _chsa_http_postprocess(self, value):
        return parse_datetime64(value)


class DateTimeWithTZ(DateTime):
    """
    'Aware' datetime, a datetime with timezone.
    """

    __visit_name__ = 'datetimewithtz'

    def __init__(self, tz):
        super(DateTimeWithTZ, self).__init__()
        self.tz = tz
        self.tzinfo = parse_timezone(tz)

    def _chsa_http_postprocess(self, value):
        return parse_datetime(value, tzinfo=self.tzinfo)


class DateTime64WithTZ(DateTime64):
    """
    'Aware' 64-bit datetime, a datetime with timezone.
    """

    __visit_name__ = 'datetime64withtz'

    def __init__(self, precision, tz):
        super(DateTime64WithTZ, self).__init__(precision)
        self.tz = tz
        self.tzinfo = parse_timezone(tz)

    def _chsa_http_postprocess(self, value):
        return parse_datetime64(value, tzinfo=self.tzinfo)


class Enum(CHTypeMixin, types.Enum):
    __visit_name__ = 'enum'

    def __init__(self, *enums, **kw):
        if not enums:
            enums = kw.get('_enums', ())  # passed as keyword

        super(Enum, self).__init__(*enums, **kw)


class Enum8(Enum):
    __visit_name__ = 'enum8'


class Enum16(Enum):
    __visit_name__ = 'enum16'


class Decimal(CHTypeMixin, types.Numeric):
    __visit_name__ = 'numeric'

    def _chsa_http_postprocess(self, value):
        if value is None:
            return None
        return decimal.Decimal(value)


class Nested(CHTypeMixin, types.TypeEngine):
    __visit_name__ = 'nested'

    def __init__(self, *columns):
        if not columns:
            raise ValueError('columns must be specified for nested type')
        self.columns = columns
        self._columns_dict = {col.name: col for col in columns}
        super(Nested, self).__init__()

    class Comparator(UserDefinedType.Comparator):
        def __getattr__(self, key):
            str_key = key.rstrip("_")
            try:
                sub = self.type._columns_dict[str_key]
            except KeyError:
                raise AttributeError(key)
            else:
                original_type = sub.type
                try:
                    sub.type = Array(sub.type)
                    expr = NestedColumn(self.expr, sub)
                    return expr
                finally:
                    sub.type = original_type

    comparator_factory = Comparator


class IPv4(CHTypeMixin, types.UserDefinedType):
    __visit_name__ = "ipv4"

    def bind_processor(self, dialect):
        def process(value):
            return text_type(value)

        return process

    def bind_expression(self, bindvalue):
        bindvalue = bindvalue._clone(maintain_key=True)
        bindvalue.expanding = False
        return func.toIPv4(bindvalue)

    class comparator_factory(types.UserDefinedType.Comparator):
        def in_(self, other):
            if isinstance(other, sa.sql.elements.BindParameter):
                other = other.value
            if not isinstance(other, IPv4Network):
                other = IPv4Network(other)

            return and_(
                self >= other[0],
                self <= other[-1]
            )

        def not_in(self, other):
            return self.notin_(other)

        def notin_(self, other):
            if isinstance(other, sa.sql.elements.BindParameter):
                other = other.value
            if not isinstance(other, IPv4Network):
                other = IPv4Network(other)

            return or_(
                self < other[0],
                self > other[-1]
            )

    def _chsa_http_postprocess(self, value):
        return IPv4Address(value)


class IPv6(CHTypeMixin, types.UserDefinedType):
    __visit_name__ = "ipv6"

    def bind_processor(self, dialect):
        def process(value):
            return text_type(value)

        return process

    def bind_expression(self, bindvalue):
        return func.toIPv6(bindvalue)

    class comparator_factory(types.UserDefinedType.Comparator):
        def in_(self, other):
            if isinstance(other, sa.sql.elements.BindParameter):
                other = other.value
            if not isinstance(other, IPv6Network):
                other = IPv6Network(other)

            return and_(
                self >= other[0],
                self <= other[-1]
            )

        def not_in(self, other):
            return self.notin_(other)

        def notin_(self, other):
            if isinstance(other, sa.sql.elements.BindParameter):
                other = other.value
            if not isinstance(other, IPv6Network):
                other = IPv6Network(other)

            return or_(
                self < other[0],
                self > other[-1]
            )

    def _chsa_http_postprocess(self, value):
        return IPv6Address(value)
