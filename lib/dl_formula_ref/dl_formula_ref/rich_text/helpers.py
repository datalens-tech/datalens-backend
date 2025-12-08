from typing import Collection

from dl_formula.core.datatype import DataType
from dl_formula_ref.texts import (
    ANY_TYPE,
    HUMAN_DATA_TYPES,
)


HIDDEN_TYPES = {
    DataType.DATETIME,
    DataType.CONST_DATETIME,
    DataType.DATETIMETZ,
    DataType.CONST_DATETIMETZ,
    DataType.TREE_STR,
    DataType.CONST_TREE_STR,
}


def get_human_data_type_list(types: Collection[DataType]) -> list[str]:
    types_set: set[DataType] = set(t.non_const_type for t in types)
    result_types = set()
    # try to find known type combinations within the given list of types
    type_combos = [type_combo for type_combo in HUMAN_DATA_TYPES if isinstance(type_combo, tuple)]
    type_combos = sorted(type_combos, key=len, reverse=True)
    for type_combo in type_combos:
        if set(type_combo).issubset(types_set):
            result_types.add(HUMAN_DATA_TYPES[type_combo])
            types_set -= set(type_combo)

    for hidden_t in HIDDEN_TYPES:
        types_set.discard(hidden_t)

    # for the rest use basic types
    result_types.update({HUMAN_DATA_TYPES[t] for t in types_set})
    result = sorted(result_types) if result_types else [ANY_TYPE]
    return result


def escape_cell(s: str) -> str:
    """
    Escape string ``s`` for usage inside a Markdown table cell.
    Pipe (``|``) characters must either be wrapped into ``<code></code>`` tags
    or replaced with HTML escape sequence ``&#124;`` so that the don't break cell structure
    """
    if "|" not in s:
        return s

    if "`" not in s:
        return s.replace("|", "&#124;")

    escaped_s = ""
    i = 0
    while True:
        # add the substring before next opening backtick
        bt_ind = s.find("`", i)
        if bt_ind == -1:
            bt_ind = len(s)
        escaped_s += s[i:bt_ind].replace("|", "&#124;")
        if bt_ind >= len(s):
            break

        use_tags = False
        # find closing backtick and check whether there are any pipes inside
        next_pipe_ind = s.find("|", bt_ind + 1)
        next_bt_ind = s.find("`", bt_ind + 1)
        if next_pipe_ind != -1 and next_pipe_ind < next_bt_ind:
            # found pipe inside backticks, so will have to use tags instead of backticks
            use_tags = True

        if use_tags:
            escaped_s = "{}{}{}{}".format(escaped_s, "<code>", s[bt_ind + 1 : next_bt_ind], "</code>")
        else:
            escaped_s = escaped_s + s[bt_ind : next_bt_ind + 1]

        i = next_bt_ind + 1

    escaped_s = escaped_s.replace("|", "&#124;")
    return escaped_s
