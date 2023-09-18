import dl_formula.definitions.operators_binary as base

from bi_connector_metrica.formula.constants import MetricaDialect as D


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.METRIKAAPI),

    # *
    base.BinaryMultNumbers.for_dialect(D.METRIKAAPI),

    # +
    base.BinaryPlusNumbers.for_dialect(D.METRIKAAPI),

    # -
    base.BinaryMinusNumbers.for_dialect(D.METRIKAAPI),

    # /
    base.BinaryDivInt.for_dialect(D.METRIKAAPI),
    base.BinaryDivFloat.for_dialect(D.METRIKAAPI),

    # <
    base.BinaryLessThan.for_dialect(D.METRIKAAPI),

    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.METRIKAAPI),

    # ==
    base.BinaryEqual.for_dialect(D.METRIKAAPI),

    # >
    base.BinaryGreaterThan.for_dialect(D.METRIKAAPI),

    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.METRIKAAPI),

    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.METRIKAAPI),

    # _==
    base.BinaryEqualInternal.for_dialect(D.METRIKAAPI),

    # _dneq
    base.BinaryEqualDenullified.for_dialect(D.METRIKAAPI),

    # and
    base.BinaryAnd.for_dialect(D.METRIKAAPI),

    # in
    base.BinaryIn.for_dialect(D.METRIKAAPI),

    # notin
    base.BinaryNotIn.for_dialect(D.METRIKAAPI),

    # or
    base.BinaryOr.for_dialect(D.METRIKAAPI),
]
