import sqlalchemy as sa
import sqlalchemy.dialects.mssql as sa_mssqlsrv

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common_datetime import DAY_SEC
import dl_formula.definitions.operators_binary as base

from dl_connector_mssql.formula.constants import MssqlDialect as D


V = TranslationVariant.make


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.MSSQLSRV),
    # %
    base.BinaryModInteger.for_dialect(D.MSSQLSRV),
    base.BinaryModFloat(
        variants=[
            V(D.MSSQLSRV, lambda left, right: left - sa.func.FLOOR(left / right) * right),
        ]
    ),
    # *
    base.BinaryMultNumbers.for_dialect(D.MSSQLSRV),
    base.BinaryMultStringConst.for_dialect(D.MSSQLSRV),
    base.BinaryMultStringNonConst(
        variants=[
            V(D.MSSQLSRV, sa.func.REPLICATE),
        ]
    ),
    # +
    base.BinaryPlusNumbers.for_dialect(D.MSSQLSRV),
    base.BinaryPlusStrings.for_dialect(D.MSSQLSRV),
    base.BinaryPlusDateInt(
        variants=[
            V(D.MSSQLSRV, lambda date, days: sa.type_coerce(sa.func.DATEADD(sa.text("day"), days, date), sa.Date)),
        ]
    ),
    base.BinaryPlusDateFloat(
        variants=[
            V(
                D.MSSQLSRV,
                lambda date, days: sa.type_coerce(sa.func.DATEADD(sa.text("day"), base.as_bigint(days), date), sa.Date),
            ),
        ]
    ),
    base.BinaryPlusDatetimeNumber(
        variants=[
            V(D.MSSQLSRV, lambda dt, days: sa.func.DATEADD(sa.text("second"), days * DAY_SEC, dt)),
        ]
    ),
    base.BinaryPlusGenericDatetimeNumber(
        variants=[
            V(D.MSSQLSRV, lambda dt, days: sa.func.DATEADD(sa.text("second"), days * DAY_SEC, dt)),
        ]
    ),
    # -
    base.BinaryMinusNumbers.for_dialect(D.MSSQLSRV),
    base.BinaryMinusDateInt(
        variants=[
            V(D.MSSQLSRV, lambda date, days: sa.type_coerce(sa.func.DATEADD(sa.text("day"), -days, date), sa.Date)),
        ]
    ),
    base.BinaryMinusDateFloat(
        variants=[
            V(
                D.MSSQLSRV,
                lambda date, days: sa.type_coerce(
                    sa.func.DATEADD(sa.text("day"), -base.as_bigint(sa.func.CEILING(days)), date), sa.Date
                ),
            ),
        ]
    ),
    base.BinaryMinusDatetimeNumber(
        variants=[
            V(D.MSSQLSRV, lambda dt, days: sa.func.DATEADD(sa.text("second"), -days * DAY_SEC, dt)),
        ]
    ),
    base.BinaryMinusGenericDatetimeNumber(
        variants=[
            V(D.MSSQLSRV, lambda dt, days: sa.func.DATEADD(sa.text("second"), -days * DAY_SEC, dt)),
        ]
    ),
    base.BinaryMinusDates(
        variants=[
            V(
                D.MSSQLSRV,
                lambda left, right: sa.cast(
                    sa.func.FLOOR(
                        sa.cast(sa.cast(left, sa.DateTime()), sa_mssqlsrv.FLOAT)
                        - sa.cast(sa.cast(right, sa.DateTime()), sa_mssqlsrv.FLOAT)
                    ),
                    sa.BIGINT(),
                ),
            ),
        ]
    ),
    base.BinaryMinusDatetimes(
        variants=[
            V(
                D.MSSQLSRV,
                lambda left, right: (
                    sa.cast(sa.func.DATEDIFF(sa.text("SECOND"), right, left), sa_mssqlsrv.FLOAT) / DAY_SEC
                ),
            ),
        ]
    ),
    base.BinaryMinusGenericDatetimes(
        variants=[
            V(
                D.MSSQLSRV,
                lambda left, right: (
                    sa.cast(sa.func.DATEDIFF(sa.text("SECOND"), right, left), sa_mssqlsrv.FLOAT) / DAY_SEC
                ),
            ),
        ]
    ),
    # /
    base.BinaryDivInt(
        variants=[
            V(D.MSSQLSRV, lambda x, y: sa.cast(x, sa.FLOAT) / y),
        ]
    ),
    base.BinaryDivFloat.for_dialect(D.MSSQLSRV),
    # <
    base.BinaryLessThan.for_dialect(D.MSSQLSRV),
    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.MSSQLSRV),
    # ==
    base.BinaryEqual.for_dialect(D.MSSQLSRV),
    # >
    base.BinaryGreaterThan.for_dialect(D.MSSQLSRV),
    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.MSSQLSRV),
    # ^
    base.BinaryPower.for_dialect(D.MSSQLSRV),
    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.MSSQLSRV),
    # _==
    base.BinaryEqualInternal.for_dialect(D.MSSQLSRV),
    # _dneq
    base.BinaryEqualDenullified.for_dialect(D.MSSQLSRV),
    # and
    base.BinaryAnd.for_dialect(D.MSSQLSRV),
    # in
    base.BinaryIn.for_dialect(D.MSSQLSRV),
    # like
    base.BinaryLike.for_dialect(D.MSSQLSRV),
    # notin
    base.BinaryNotIn.for_dialect(D.MSSQLSRV),
    # notlike
    base.BinaryNotLike.for_dialect(D.MSSQLSRV),
    # or
    base.BinaryOr.for_dialect(D.MSSQLSRV),
]
