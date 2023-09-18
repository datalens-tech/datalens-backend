from dl_db_testing.database.engine_wrapper import EngineWrapperBase


class TestingEngineWrapper(EngineWrapperBase):
    """
    Dummy engine wrapper to use in tests without a proper DB and an sqlalchemy dialect
    """

    URL_PREFIX = "bi_testing"

    def __attrs_post_init__(self) -> None:
        pass
