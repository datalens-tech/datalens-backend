import sqlalchemy as sa

import bi_formula.definitions.operators_binary as base
from bi_formula.definitions.base import TranslationVariant
from bi_formula.definitions.common import raw_sql
from bi_formula.definitions.common_datetime import DAY_SEC, DAY_USEC

from bi_connector_mysql.formula.constants import MySQLDialect as D


V = TranslationVariant.make


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.MYSQL),

    # %
    base.BinaryModInteger.for_dialect(D.MYSQL),
    base.BinaryModFloat.for_dialect(D.MYSQL),

    # *
    base.BinaryMultNumbers.for_dialect(D.MYSQL),
    base.BinaryMultStringConst.for_dialect(D.MYSQL),
    base.BinaryMultStringNonConst(variants=[
        V(D.MYSQL, sa.func.REPEAT),
    ]),

    # +
    base.BinaryPlusNumbers.for_dialect(D.MYSQL),
    base.BinaryPlusStrings.for_dialect(D.MYSQL),
    base.BinaryPlusDateInt(variants=[
        V(D.MYSQL, lambda date, days: sa.func.FROM_DAYS(sa.func.TO_DAYS(date) + days)),
    ]),
    base.BinaryPlusDateFloat(variants=[
        V(D.MYSQL, lambda date, days: sa.func.FROM_DAYS(sa.func.TO_DAYS(date) + base.as_bigint(days))),
    ]),
    base.BinaryPlusDatetimeNumber(variants=[
        V(D.MYSQL, lambda dt, days: sa.func.FROM_UNIXTIME(sa.func.UNIX_TIMESTAMP(dt) + days * DAY_SEC)),
    ]),
    base.BinaryPlusGenericDatetimeNumber(variants=[
        V(D.MYSQL, lambda dt, days: sa.func.TIMESTAMPADD(raw_sql('microsecond'), base.as_bigint(days * DAY_USEC), dt)),
    ]),

    # -
    base.BinaryMinusNumbers.for_dialect(D.MYSQL),
    base.BinaryMinusDateInt(variants=[
        V(D.MYSQL, lambda date, days: sa.func.FROM_DAYS(sa.func.TO_DAYS(date) - days)),
    ]),
    base.BinaryMinusDateFloat(variants=[
        V(D.MYSQL, lambda date, days: sa.func.FROM_DAYS(sa.func.TO_DAYS(date) - base.as_bigint(sa.func.CEIL(days)))),
    ]),
    base.BinaryMinusDatetimeNumber(variants=[
        V(D.MYSQL, lambda dt, days: sa.func.FROM_UNIXTIME(sa.func.UNIX_TIMESTAMP(dt) - days * DAY_SEC)),
    ]),
    base.BinaryMinusGenericDatetimeNumber(variants=[
        V(D.MYSQL, lambda dt, days: sa.func.TIMESTAMPADD(raw_sql('microsecond'), base.as_bigint(-days * DAY_USEC), dt)),
    ]),
    base.BinaryMinusDates.for_dialect(D.MYSQL),
    base.BinaryMinusDatetimes(variants=[
        V(D.MYSQL, lambda left, right: (sa.func.UNIX_TIMESTAMP(left) - sa.func.UNIX_TIMESTAMP(right)) / DAY_SEC),
    ]),
    base.BinaryMinusGenericDatetimes(variants=[
        V(D.MYSQL, lambda left, right: sa.func.TIMESTAMPDIFF(raw_sql('second'), right, left) / DAY_SEC),
    ]),

    # /
    base.BinaryDivInt.for_dialect(D.MYSQL),
    base.BinaryDivFloat.for_dialect(D.MYSQL),

    # <
    base.BinaryLessThan.for_dialect(D.MYSQL),

    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.MYSQL),

    # ==
    base.BinaryEqual.for_dialect(D.MYSQL),

    # >
    base.BinaryGreaterThan.for_dialect(D.MYSQL),

    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.MYSQL),

    # ^
    base.BinaryPower.for_dialect(D.MYSQL),

    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.MYSQL),

    # _==
    base.BinaryEqualInternal.for_dialect(D.MYSQL),

    # _dneq
    base.BinaryEqualDenullified.for_dialect(D.MYSQL),

    # and
    base.BinaryAnd.for_dialect(D.MYSQL),

    # in
    base.BinaryIn.for_dialect(D.MYSQL),

    # like
    base.BinaryLike.for_dialect(D.MYSQL),

    # notin
    base.BinaryNotIn.for_dialect(D.MYSQL),

    # notlike
    base.BinaryNotLike.for_dialect(D.MYSQL),

    # or
    base.BinaryOr.for_dialect(D.MYSQL),
]
