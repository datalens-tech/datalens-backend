from __future__ import annotations

import difflib
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
)

import attr
import yaml

from dl_core.us_entry import (
    USEntry,
    USMigrationEntry,
)
from dl_core.us_manager.us_manager import USManagerBase


@attr.s(frozen=True, auto_attribs=True)
class DictFieldsDiff:
    added: Sequence[str]
    modified: Sequence[str]
    removed: Sequence[str]

    def __bool__(self) -> bool:
        return any(attr.asdict(self).values())

    def short_str(self) -> str:
        parts = [f"{f_name}={f_val}" for f_name, f_val in attr.asdict(self).items() if f_val]
        return f"({';'.join(parts)})"


@attr.s(frozen=True, auto_attribs=True)
class EntryFieldsDiff:
    data: DictFieldsDiff
    unversioned_data: DictFieldsDiff
    meta: DictFieldsDiff

    def short_str(self) -> str:
        parts = [f"{f_name}={f_val.short_str()}" for f_name, f_val in attr.asdict(self, recurse=False).items() if f_val]
        return f"Diff({' '.join(parts)})"

    def __bool__(self) -> bool:
        return any(attr.asdict(self).values())


def get_dict_top_level_diff(a: Dict[str, Any], b: Dict[str, Any]) -> DictFieldsDiff:
    a_fields = set(a.keys())
    b_fields = set(b.keys())

    return DictFieldsDiff(
        removed=tuple(sorted(a_fields - b_fields)),
        added=tuple(sorted(b_fields - a_fields)),
        modified=tuple(sorted(field for field in a_fields & b_fields if a[field] != b[field])),
    )


def get_pre_save_top_level_dict(entry: USMigrationEntry) -> EntryFieldsDiff:
    us_resp = entry._us_resp
    return EntryFieldsDiff(
        data=get_dict_top_level_diff(us_resp["data"], entry.data),  # type: ignore  # TODO: fix
        unversioned_data=get_dict_top_level_diff(us_resp.get("unversionedData") or {}, entry.unversioned_data),  # type: ignore  # TODO: fix
        meta=get_dict_top_level_diff(us_resp["meta"], entry.meta),  # type: ignore  # TODO: fix
    )


def _colorize_diff(text: str) -> str:
    from pygments import (
        formatters,
        highlight,
        lexers,
    )

    return highlight(
        text,
        lexer=lexers.DiffLexer(),
        # formatter=formatters.TerminalFormatter(),
        # Try also:
        formatter=formatters.Terminal256Formatter(),
    )


def dump_yaml_for_diff(value: Any) -> List[str]:
    return yaml.safe_dump(
        value,
        default_flow_style=False,
        allow_unicode=True,
        encoding=None,
    ).splitlines()


# https://github.com/venthur/python-gron (MIT)
# minimized
def dump_gron(
    value: Any,
    root_name: str = "",
    init_values: bool = False,
    line_tpl: str = "{name} = {value};",
    sort_keys: bool = False,
) -> Iterable[str]:
    def quote_key(key: Any) -> str:
        return f".{key}"

    def gron_walk(node: Any, name: str) -> Iterable[str]:
        if node is None:
            yield line_tpl.format(name=name, value="null")
            return
        if isinstance(node, bool):
            yield line_tpl.format(name=name, value="true" if node else "false")
            return
        if isinstance(node, (str, bytes)):
            yield line_tpl.format(name=name, value='"{}"'.format(node))  # type: ignore  # TODO: fix
            return
        if isinstance(node, dict):
            if init_values:
                yield line_tpl.format(name=name, value="{}")
            items = node.items()
            if sort_keys:
                items = sorted(items)  # type: ignore  # TODO: fix
            for key, value in items:
                children = gron_walk(value, name="{}{}".format(name, quote_key(key)))
                for child in children:
                    yield child
            return
        if isinstance(node, (list, tuple)):
            if init_values:
                yield line_tpl.format(name=name, value="[]")
            for idx, value in enumerate(node):
                children = gron_walk(value, name="{}[{}]".format(name, idx))
                for child in children:
                    yield child
            return
        yield line_tpl.format(
            name=name,
            value=str(node),
        )
        return

    return gron_walk(value, name=root_name)


def make_diff(  # type: ignore  # TODO: fix
    value_a, value_b, unified_n: int = 1, dumper=lambda value: dump_gron(value, sort_keys=True), colorize: bool = True
):
    value_a_lines = list(dumper(value_a))
    value_b_lines = list(dumper(value_b))
    diff_lines = difflib.unified_diff(
        value_a_lines,
        value_b_lines,
        n=unified_n,
    )
    result = "\n".join(line for line in diff_lines if not line.startswith("---") and not line.startswith("+++"))
    result = result.strip()

    if colorize:
        try:
            result = _colorize_diff(result).strip()
        except Exception:
            pass
    return result


def get_diff_text(  # type: ignore  # TODO: fix
    entry: USEntry,
    us_manager: USManagerBase,
    unified_n: int = 1,  # recommended: 1 for gron, 5 for yaml
    dumper=dump_gron,
    colorize: bool = True,
) -> Optional[str]:
    if not isinstance(entry, USMigrationEntry):
        return None

    us_resp = entry._us_resp
    value_a = dict(
        data=dict(us_resp["data"]),  # type: ignore  # TODO: fix
        unversioned_data=dict(us_resp.get("unversionedData") or {}),  # type: ignore  # TODO: fix
        meta=us_resp["meta"],  # type: ignore  # TODO: fix
    )
    value_b = dict(
        data=dict(us_manager.dump_data(entry)),
        unversioned_data=dict(getattr(entry, "unversioned_data", {})),
        meta=dict(entry.meta),
    )

    result = make_diff(value_a=value_a, value_b=value_b, unified_n=unified_n, dumper=dumper, colorize=colorize)
    return result
