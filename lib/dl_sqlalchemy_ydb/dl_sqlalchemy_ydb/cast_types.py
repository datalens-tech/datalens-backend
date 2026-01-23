import sqlalchemy as sa


class Double(sa.FLOAT):
    """
    Partial `Double` type implementation for CAST expressions only
    """

    __visit_name__ = "Double"


class Utf8(sa.TEXT):
    """
    Partial `Utf8` type implementation for CAST expressions only
    """

    __visit_name__ = "Utf8"


class Uuid(sa.TEXT):
    """
    Partial `Utf8` type implementation for CAST expressions only
    """

    __visit_name__ = "Uuid"
