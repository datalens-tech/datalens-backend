from typing import TypeVar

from dl_formula.definitions.base import MultiVariantTranslation

from dl_connector_bundle_chs3.file.formula.constants import FileS3Dialect as D


_FUNC_TV = TypeVar("_FUNC_TV", bound=MultiVariantTranslation)


def clickhouse_funcs_for_file_dialect(funcs_list: list[_FUNC_TV]) -> list[MultiVariantTranslation]:
    return [clickhouse_func.for_another_dialect(D.FILE) for clickhouse_func in funcs_list]
