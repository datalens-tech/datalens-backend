import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.operators_binary as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.STARROCKS),
    # %
    base.BinaryModInteger.for_dialect(D.STARROCKS),
    base.BinaryModFloat.for_dialect(D.STARROCKS),
    # *
    base.BinaryMultNumbers.for_dialect(D.STARROCKS),
    base.BinaryMultStringConst.for_dialect(D.STARROCKS),
    base.BinaryMultStringNonConst(
        variants=[
            V(D.STARROCKS, sa.func.REPEAT),
        ]
    ),
    # +
    base.BinaryPlusNumbers.for_dialect(D.STARROCKS),
    base.BinaryPlusStrings.for_dialect(D.STARROCKS),
    # TODO: BI-7171 BinaryPlusDateInt, BinaryPlusDateFloat: DAYS_ADD returns datetime instead of date
    base.BinaryPlusDateInt(
        variants=[
            V(D.STARROCKS, lambda date, days: sa.func.DAYS_ADD(date, days)),
        ]
    ),
    base.BinaryPlusDateFloat(
        variants=[
            V(D.STARROCKS, lambda date, days: sa.func.DAYS_ADD(date, base.as_bigint(days))),
        ]
    ),
    # TODO: BI-7171 BinaryPlusDatetimeNumber, BinaryPlusGenericDatetimeNumber
    base.BinaryMinusNumbers.for_dialect(D.STARROCKS),
    # TODO: BI-7171 BinaryMinusDateInt, BinaryMinusDateFloat: DAYS_SUB returns datetime instead of date
    base.BinaryMinusDateInt(
        variants=[
            V(D.STARROCKS, lambda date, days: sa.func.DAYS_SUB(date, days)),
        ]
    ),
    base.BinaryMinusDateFloat(
        variants=[
            V(D.STARROCKS, lambda date, days: sa.func.DAYS_SUB(date, base.as_bigint(sa.func.CEIL(days)))),
        ]
    ),
    # TODO: BI-7171 BinaryMinusDatetimeNumber, BinaryMinusGenericDatetimeNumber
    base.BinaryMinusDates(
        variants=[
            V(D.STARROCKS, sa.func.DATEDIFF),
        ]
    ),
    # TODO: BI-7171 BinaryMinusDatetimes, BinaryMinusGenericDatetimes
    # /
    base.BinaryDivInt.for_dialect(D.STARROCKS),
    base.BinaryDivFloat.for_dialect(D.STARROCKS),
    # <
    base.BinaryLessThan.for_dialect(D.STARROCKS),
    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.STARROCKS),
    # ==
    base.BinaryEqual.for_dialect(D.STARROCKS),
    # >
    base.BinaryGreaterThan.for_dialect(D.STARROCKS),
    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.STARROCKS),
    # ^
    base.BinaryPower.for_dialect(D.STARROCKS),
    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.STARROCKS),
    # _==
    base.BinaryEqualInternal.for_dialect(D.STARROCKS),
    # _dneq
    base.BinaryEqualDenullified.for_dialect(D.STARROCKS),
    # and
    base.BinaryAnd.for_dialect(D.STARROCKS),
    # in
    base.BinaryIn.for_dialect(D.STARROCKS),
    # like
    base.BinaryLike.for_dialect(D.STARROCKS),
    # notin
    base.BinaryNotIn.for_dialect(D.STARROCKS),
    # notlike
    base.BinaryNotLike.for_dialect(D.STARROCKS),
    # or
    base.BinaryOr.for_dialect(D.STARROCKS),
]
