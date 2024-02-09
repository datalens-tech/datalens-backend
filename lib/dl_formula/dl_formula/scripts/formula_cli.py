from __future__ import annotations

import argparse
from collections import defaultdict
import inspect
import os
import sys
from typing import (
    TYPE_CHECKING,
    Generator,
    List,
    Optional,
    Tuple,
    Union,
)

from dl_formula.core import exc
from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import (
    get_all_basic_dialects,
    get_dialect_from_str,
)
from dl_formula.core.dialect import DialectCombo
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.base import (
    MultiVariantTranslation,
    NodeTranslation,
)
from dl_formula.definitions.registry import OPERATION_REGISTRY
import dl_formula.dot
import dl_formula.inspect.expression
import dl_formula.inspect.function
from dl_formula.loader import load_formula_lib
from dl_formula.parser.factory import get_parser
from dl_formula.slicing.schema import (
    AggregateFunctionLevelBoundary,
    NonFieldsBoundary,
    SliceSchema,
    TopLevelBoundary,
    WindowFunctionLevelBoundary,
)
from dl_formula.slicing.slicer import FormulaSlicer
import dl_formula.translation.translator

from .common import (
    csv_type,
    make_graphviz_graph,
)

if TYPE_CHECKING:
    import dl_formula.core.nodes as nodes


formula_parser = get_parser()


def dialect_type(s: Union[str, DialectCombo]) -> Union[DialectCombo, int]:
    if isinstance(s, str):
        res = D.EMPTY
        for it in s.split(","):
            res |= get_dialect_from_str(it.upper())
        return res
    return s


list_type = csv_type(str)


parser = argparse.ArgumentParser(prog="Formula command line tool")
subparsers = parser.add_subparsers(title="command", dest="command")

text_parser = argparse.ArgumentParser(add_help=False)
text_parser.add_argument("text", help="formula text")
optional_text_parser = argparse.ArgumentParser(add_help=False)
optional_text_parser.add_argument("optional_text", nargs="?", help="formula text")

dialect_parser = argparse.ArgumentParser(add_help=False)
dialect_parser.add_argument(
    "--dialect",
    required=True,
    type=dialect_type,
    help="SQL dialect (comma-separated list, use ALL for combination of all dialects)",
)

category_parser = argparse.ArgumentParser(add_help=False)
category_parser.add_argument("--category", help="Locale")

supperr_parser = argparse.ArgumentParser(add_help=False)
supperr_parser.add_argument("--suppress-errors", action="store_true", help="Print ERROR token instead of failing")

fields_parser = argparse.ArgumentParser(add_help=False)
fields_parser.add_argument("--fields", type=list_type, help="List of fields", default=[])

diff_parser = argparse.ArgumentParser(add_help=False)
diff_parser.add_argument("--diff", action="store_true", help="Diff mode")

trans_parser = subparsers.add_parser(
    "translate",
    parents=[optional_text_parser, dialect_parser, supperr_parser],
    help="Translate formula into an SQL expression",
)
trans_parser.add_argument("--table", help="Name of database table")
trans_parser.add_argument("--unknown-funcs", action="store_true", help="Allow usage of unknown functions")

parse_parser = subparsers.add_parser(
    "parse", parents=[optional_text_parser, supperr_parser], help="Parse formula and print its internal representation"
)
parse_parser.add_argument("--pretty", action="store_true", help="Print in pretty multiline format")
parse_parser.add_argument("--pos", type=int, help="Print node at the given position")
parse_parser.add_argument("--with-meta", action="store_true", help="Print meta for nodes")

split_parser = subparsers.add_parser(
    "split", parents=[text_parser, diff_parser], help="Split formula into nodes and print them by nesting level"
)

graph_parser = subparsers.add_parser("graph", parents=[text_parser], help="Generate GraphViz DOT graph")
graph_parser.add_argument("--view", action="store_true", help="Show rendered graph (requires GUI)")
graph_parser.add_argument("--render-to", help="Render graph to specified file", default=None)

dialect_parser = subparsers.add_parser("dialects", help="List available basic dialects")

goto_parser = subparsers.add_parser("goto", help="Open PyCharm at the function definition")
goto_parser.add_argument("func", help="function name")
goto_parser.add_argument("--show", action="store_true", help="just print file name and line number")

slice_parser = subparsers.add_parser(
    "slice", parents=[text_parser, diff_parser], help="Parse formula slice it into translation levels"
)
slice_parser.add_argument("--levels", type=list_type, help="Formula levels")

registry_parser = subparsers.add_parser("registry", help="List function definitions in registry")


ERROR_TOKEN = "#ERROR"


class FormulaCliTool:
    @staticmethod
    def _stdin_lines() -> Generator[str, None, None]:
        lines = sys.stdin.readlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue

            yield line

    @classmethod
    def parse(
        cls, text: str, pretty: bool, suppress_errors: bool, pos: Optional[int] = None, with_meta: bool = False
    ) -> None:
        if not text:
            # bulk mode, read from STDIN
            for line in cls._stdin_lines():
                cls.parse(text=line, pretty=False, suppress_errors=suppress_errors, pos=pos)
            return

        try:
            tree = formula_parser.parse(text)
        except exc.ParseError:
            if suppress_errors:
                print(ERROR_TOKEN)
                return
            else:
                raise

        if pos is not None:
            tree = tree.get_by_pos(pos)  # type: ignore  # TODO: fix

        if pretty:
            text = tree.pretty(with_meta=with_meta)
        else:
            text = tree.stringify(with_meta=with_meta)
        print(text)

    @classmethod
    def split(cls, text: str, diff: bool) -> None:
        formula = formula_parser.parse(text)

        chars: list[list[str]] = []

        def _mark_positions(node: nodes.FormulaItem, level: int = 0) -> None:
            if len(chars) == level:
                chars.append([" " for _ in range(len(text))])
            if node.pos_range != (None, None):
                start, stop = node.pos_range
                assert start is not None
                assert stop is not None
                for i in range(start, stop):
                    chars[level][i] = text[i]
                    if diff and level > 0:
                        chars[level - 1][i] = "▫"

            for child in node.children:
                _mark_positions(child, level + 1)

        _mark_positions(formula)
        for num, line in enumerate(chars):
            print("{0:>2}: {1}".format(num, "".join(line)))

    @staticmethod
    def graph(text: str, render_to: str, view: bool) -> None:
        formula = formula_parser.parse(text)
        dot = dl_formula.dot.translate(formula)
        make_graphviz_graph(dot, render_to=render_to, view=view)

    @classmethod
    def translate(
        cls, text: str, tablename: Optional[str], dialect: DialectCombo, unknown_funcs: bool, suppress_errors: bool
    ) -> None:
        if not text:
            # bulk mode, read from STDIN
            for line in cls._stdin_lines():
                cls.translate(
                    text=line,
                    tablename=tablename,
                    dialect=dialect,
                    unknown_funcs=unknown_funcs,
                    suppress_errors=suppress_errors,
                )
            return

        try:
            formula = formula_parser.parse(text)

            field_list = sorted(set(f.name for f in dl_formula.inspect.expression.used_fields(formula)))
            field_types = {f: DataType.NULL for f in field_list}
            if tablename:
                field_names = {f: (tablename, f) for f in field_list}
            else:
                field_names = {f: (f,) for f in field_list}  # type: ignore  # TODO: fix

            raw_sql = dl_formula.translation.translator.translate_and_compile(
                formula,
                dialect=dialect,
                restrict_funcs=not unknown_funcs,
                field_types=field_types,
                field_names=field_names,  # type: ignore  # TODO: fix
            )
            print(raw_sql)

        except (exc.ParseError, exc.TranslationError):
            if suppress_errors:
                print(ERROR_TOKEN)
                return
            else:
                raise

    @staticmethod
    def list_dialects() -> None:
        print(
            "\n".join(
                [
                    d.common_name_and_version
                    for d in sorted(get_all_basic_dialects(), key=lambda el: el.single_bit.orderable)
                ]
            )
        )

    @staticmethod
    def _get_func_base_class(name: str) -> Optional[type]:  # type: ignore  # TODO: fix
        """Find the first (base) class for the given function"""
        name = name.lower()
        for _i, definition in OPERATION_REGISTRY.items():
            if definition.name.lower() == name:  # type: ignore  # TODO: fix
                item = definition
                break
        else:
            return None

        mro = inspect.getmro(type(item))  # `cls` itself plus all its bases
        for earliest_super_cls in reversed(mro):
            if (
                issubclass(earliest_super_cls, MultiVariantTranslation)
                and (earliest_super_cls.name or "").lower() == name
            ):
                return earliest_super_cls

    @classmethod
    def _get_func_source_info(cls, name: str) -> Tuple[str, int]:
        """Get file name and line number for given function"""
        func_cls = cls._get_func_base_class(name)
        filename = inspect.getsourcefile(func_cls)  # type: ignore  # TODO: fix
        lineno = inspect.getsourcelines(func_cls)[1]  # type: ignore  # TODO: fix
        return filename, lineno  # type: ignore  # TODO: fix

    @classmethod
    def goto_func(cls, name: str, show: bool) -> None:
        editor_exe = os.environ.get("PYCHARM", "pycharm")
        filename, lineno = cls._get_func_source_info(name)
        if show:
            print(filename, lineno)
        else:
            os.system('{0} --line {2} "{1}"'.format(editor_exe, filename, lineno))

    @classmethod
    def slice(cls, text: str, levels: List[str], diff: bool = False) -> None:
        boundaries = {
            "aggregate": AggregateFunctionLevelBoundary(name="aggregate"),
            "window": WindowFunctionLevelBoundary(name="window"),
            "nonfields": NonFieldsBoundary(name="nonfields"),
            "toplevel": TopLevelBoundary(name="toplevel"),
        }

        formula = formula_parser.parse(text)
        slicer = FormulaSlicer(slice_schema=SliceSchema(levels=[boundaries[level_name] for level_name in levels]))
        sliced_formula = slicer.slice_formula(formula=formula)

        if diff:
            char_table = []  # type: ignore  # TODO: fix
            for i in reversed(range(len(levels))):
                char_line = [" " for _ in range(len(text))]
                for _alias, node in sorted(sliced_formula.slices[i].aliased_nodes.items()):
                    char_line[node.start_pos : node.end_pos] = list(text[node.start_pos : node.end_pos])
                    if char_table:
                        for j in range(node.start_pos, node.end_pos):  # type: ignore  # TODO: fix
                            char_table[0][j] = "▫"
                char_table.insert(0, char_line)

            for i, char_line in reversed(list(enumerate(char_table))):
                print("{0:<15}{1}".format(levels[i], "".join(char_line)))
            print()

        else:
            for slice in reversed(sliced_formula.slices):
                print(f"------- {slice.name}")
                for alias, node in sorted(slice.aliased_nodes.items()):
                    print(f"[{alias}]:  {node}")
                print()

    @classmethod
    def print_registry(cls) -> None:
        items_by_module: dict[str, list[NodeTranslation]] = defaultdict(list)
        for _key, item in OPERATION_REGISTRY.items():
            filename = inspect.getsourcefile(type(item))
            assert filename is not None
            items_by_module[filename].append(item)

        prev_name: Optional[str]
        for filename, item_list in sorted(items_by_module.items()):
            print(f"{filename}:")
            prev_name = None
            for item in item_list:
                if item.name != prev_name:
                    prev_name = item.name
                    print(f"    # {item.name}")
                print(f"    {type(item).__name__},")

    @classmethod
    def run(cls, args: argparse.Namespace) -> None:
        tool = cls()

        if args.command == "parse":
            tool.parse(
                text=args.optional_text,
                pretty=args.pretty,
                suppress_errors=args.suppress_errors,
                pos=args.pos,
                with_meta=args.with_meta,
            )

        elif args.command == "split":
            tool.split(text=args.text, diff=args.diff)

        elif args.command == "graph":
            tool.graph(text=args.text, render_to=args.render_to, view=args.view)

        elif args.command == "translate":
            tool.translate(
                text=args.optional_text,
                tablename=args.table,
                dialect=args.dialect,
                unknown_funcs=args.unknown_funcs,
                suppress_errors=args.suppress_errors,
            )

        elif args.command == "dialects":
            tool.list_dialects()

        elif args.command == "goto":
            tool.goto_func(name=args.func, show=args.show)

        elif args.command == "slice":
            tool.slice(text=args.text, levels=args.levels, diff=args.diff)

        elif args.command == "registry":
            tool.print_registry()


def main() -> None:
    load_formula_lib()
    FormulaCliTool.run(parser.parse_args())


if __name__ == "__main__":
    main()
