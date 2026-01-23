import sqlalchemy as sa


class Double(sa.Float):
    """
    Partial `Double` type implementation for CAST expressions only
    """

    __visit_name__ = "Double"


class Utf8(sa.Text):
    """
    Partial `Utf8` type implementation for CAST expressions only
    """

    __visit_name__ = "Utf8"
