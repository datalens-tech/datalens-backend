class ConstContainer:
    """
    Base class for all const containers.
    """


class DatasetConstraints(ConstContainer):
    """
    Various dataset limits
    """

    # TODO: Figure out the best place to put this stuff
    FIELD_COUNT_LIMIT_SOFT = 1200
    FIELD_COUNT_LIMIT_HARD = 1250


class DataAPILimits(ConstContainer):
    """
    Data limits
    """

    DEFAULT_SUBQUERY_LIMIT = 100_000
    DEFAULT_SOURCE_DB_LIMIT = 1_000_000
    PREVIEW_ROW_LIMIT = 1000

    # endpoint-specific
    PREVIEW_API_DEFAULT_ROW_COUNT_HARD_LIMIT = 100_000
    DATA_API_DEFAULT_ROW_COUNT_HARD_LIMIT = 1_000_000
    PIVOT_API_DEFAULT_ROW_COUNT_HARD_LIMIT = 100_000
