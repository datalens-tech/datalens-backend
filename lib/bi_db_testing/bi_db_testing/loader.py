from bi_db_testing.db_testing_connectors import register_all_connectors


def load_bi_db_testing() -> None:
    """Initialize the library"""
    register_all_connectors()
