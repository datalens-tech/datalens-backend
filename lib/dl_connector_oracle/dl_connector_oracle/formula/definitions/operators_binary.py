import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.operators_binary as base

from dl_connector_oracle.formula.constants import OracleDialect as D


V = TranslationVariant.make


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.ORACLE),
    # %
    base.BinaryModInteger.for_dialect(D.ORACLE),
    base.BinaryModFloat.for_dialect(D.ORACLE),
    # *
    base.BinaryMultNumbers.for_dialect(D.ORACLE),
    base.BinaryMultStringConst.for_dialect(D.ORACLE),
    base.BinaryMultStringNonConst(
        variants=[
            V(
                D.ORACLE,
                lambda text, size: sa.func.SUBSTR(
                    sa.func.LPAD(".", size * sa.func.LENGTH(text) + 1, text), 1, size * sa.func.LENGTH(text)
                ),
            ),
        ]
    ),
    # +
    base.BinaryPlusNumbers.for_dialect(D.ORACLE),
    base.BinaryPlusStrings.for_dialect(D.ORACLE),
    base.BinaryPlusDateInt(
        variants=[
            V(D.ORACLE, lambda date, days: date + days),
        ]
    ),
    base.BinaryPlusDateFloat(
        variants=[
            V(D.ORACLE, lambda date, days: date + days),
        ]
    ),
    base.BinaryPlusDatetimeNumber(
        variants=[
            V(D.ORACLE, lambda dt, days: dt + days),
        ]
    ),
    base.BinaryPlusGenericDatetimeNumber(
        variants=[
            V(D.ORACLE, lambda dt, days: dt + days),
        ]
    ),
    # -
    base.BinaryMinusNumbers.for_dialect(D.ORACLE),
    base.BinaryMinusDateInt(
        variants=[
            V(D.ORACLE, lambda date, days: date - days),
        ]
    ),
    base.BinaryMinusDateFloat(
        variants=[
            V(D.ORACLE, lambda date, days: date - days),
        ]
    ),
    base.BinaryMinusDatetimeNumber(
        variants=[
            V(D.ORACLE, lambda dt, days: dt - days),
        ]
    ),
    base.BinaryMinusGenericDatetimeNumber(
        variants=[
            V(D.ORACLE, lambda dt, days: dt - days),
        ]
    ),
    base.BinaryMinusDates.for_dialect(D.ORACLE),
    base.BinaryMinusDatetimes(
        variants=[
            V(D.ORACLE, lambda left, right: left - right),
        ]
    ),
    base.BinaryMinusGenericDatetimes(
        variants=[
            V(D.ORACLE, lambda left, right: left - right),
        ]
    ),
    # /
    base.BinaryDivInt.for_dialect(D.ORACLE),
    base.BinaryDivFloat.for_dialect(D.ORACLE),
    # <
    base.BinaryLessThan.for_dialect(D.ORACLE),
    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.ORACLE),
    # ==
    base.BinaryEqual.for_dialect(D.ORACLE),
    # >
    base.BinaryGreaterThan.for_dialect(D.ORACLE),
    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.ORACLE),
    # ^
    base.BinaryPower.for_dialect(D.ORACLE),
    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.ORACLE),
    # _==
    base.BinaryEqualInternal.for_dialect(D.ORACLE),
    # _dneq
    base.BinaryEqualDenullified.for_dialect(D.ORACLE),
    # and
    base.BinaryAnd.for_dialect(D.ORACLE),
    # in
    base.BinaryIn.for_dialect(D.ORACLE),
    # like
    base.BinaryLike.for_dialect(D.ORACLE),
    # notin
    base.BinaryNotIn.for_dialect(D.ORACLE),
    # notlike
    base.BinaryNotLike.for_dialect(D.ORACLE),
    # or
    base.BinaryOr.for_dialect(D.ORACLE),
]
