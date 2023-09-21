from dl_db_testing.db_testing_connectors import register_all_connectors


def load_db_testing_lib() -> None:
    """Initialize the library"""
    register_all_connectors()
