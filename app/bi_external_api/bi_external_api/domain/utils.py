from typing import Optional, Sequence


def ensure_tuple(col: Optional[Sequence]) -> Optional[tuple]:
    if col is None:
        return None
    if isinstance(col, tuple):
        return col
    if isinstance(col, list):
        return tuple(col)
    else:
        raise TypeError()


def ensure_tuple_of_tuples(col: Optional[Sequence[Sequence]]) -> Optional[tuple[Optional[tuple], ...]]:
    if col is None:
        return None
    if isinstance(col, tuple) and all(isinstance(sub_col, tuple) for sub_col in col):
        return col
    if isinstance(col, (list, tuple)):
        return tuple(
            sub_col if isinstance(sub_col, tuple) else tuple(sub_col)
            for sub_col in col
        )
    else:
        raise TypeError()
