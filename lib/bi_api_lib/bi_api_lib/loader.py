from bi_formula.loader import load_bi_formula

from bi_core.loader import load_bi_core

from bi_api_lib.app_connectors import register_all_connectors


def load_bi_api_lib() -> None:
    load_bi_formula()
    load_bi_core()
    register_all_connectors()
