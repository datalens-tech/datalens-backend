import attr

from dl_formula.core.dialect import (
    DialectCombo,
    dialect_combo_is_supported,
)
from dl_formula_ref.registry.env import GenerationEnvironment
from dl_formula_ref.registry.note import Note


@attr.s
class FunctionExtension:
    category_name: str = attr.ib(kw_only=True)
    function_name: str = attr.ib(kw_only=True)
    notes: tuple[Note, ...] = attr.ib(kw_only=True)
    dialect_combo: DialectCombo = attr.ib(kw_only=True)


_FUNCTION_EXTENSION_REGISTRY: dict[tuple[str, str], list[FunctionExtension]] = {}


def register_function_extension(func_ext: FunctionExtension) -> None:
    key = (func_ext.category_name, func_ext.function_name)
    found_exts = _FUNCTION_EXTENSION_REGISTRY.get(key, [])
    if func_ext not in found_exts:
        _FUNCTION_EXTENSION_REGISTRY[key] = found_exts + [func_ext]


def get_function_extensions(category: str, name: str, env: GenerationEnvironment) -> list[FunctionExtension]:
    key = (category, name)
    extensions = _FUNCTION_EXTENSION_REGISTRY.get(key, [])
    extensions = [
        ext
        for ext in extensions
        if dialect_combo_is_supported(supported=env.supported_dialects, current=ext.dialect_combo)
    ]
    return extensions


def get_function_extension_notes(category: str, name: str, env: GenerationEnvironment) -> list[Note]:
    return [
        note for func_ext in get_function_extensions(category=category, name=name, env=env) for note in func_ext.notes
    ]
