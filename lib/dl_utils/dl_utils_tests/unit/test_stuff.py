from __future__ import annotations

from dl_utils.utils import join_in_chunks


def test_stuff():
    assert True


def test_join_in_chunks():
    # TODO: `hypothesis` test over these parameters
    values = list(map(str, range(1000)))
    sep = "_"
    max_len = 10
    # ...
    tst_result = list(
        join_in_chunks(
            values,
            sep,
            max_len=max_len,
        )
    )
    effective_max_len = max(max_len, max(len(val) + 1 for val in values))
    assert all(len(piece) < effective_max_len for piece in tst_result)
    assert [item for piece in tst_result for item in piece.split(sep)] == values
