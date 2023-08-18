from bi_core.core_connectors import register_all_connectors
from bi_core.core_data_processors import register_all_data_processor_plugins


def load_bi_core() -> None:
    """Initialize the library"""
    register_all_connectors()
    register_all_data_processor_plugins()
