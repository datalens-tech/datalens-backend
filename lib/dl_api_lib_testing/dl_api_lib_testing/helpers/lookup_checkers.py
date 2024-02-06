import datetime
from typing import (
    Any,
    Callable,
)


def read_date(s: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(s)
    except ValueError:
        return datetime.datetime.fromisoformat(s).date()


def check_at_date_data(
    data_rows: list[list[Any]],
    date_idx: int,
    value_idx: int,
    ago_idx: int,
    ago_date_callable: Callable[[datetime.date], datetime.date],
    allow_missing_date_values: bool = False,
) -> None:
    assert len(data_rows) > 0
    value_by_date = {read_date(row[date_idx]): row[value_idx] for row in data_rows}
    rows_checked = 0

    for _row_idx, row in enumerate(data_rows):
        cur_date = read_date(row[date_idx])
        ago_date = ago_date_callable(cur_date)
        expected_ago_value = value_by_date.get(ago_date)
        actual_ago_value = row[ago_idx]

        if expected_ago_value is None:
            if allow_missing_date_values:
                pass  # Do not check in this case
            else:
                assert actual_ago_value is None
        else:
            assert actual_ago_value == expected_ago_value

        rows_checked += 1

    # Make sure that rows were checked
    assert rows_checked > 5


def check_ago_data(
    data_rows: list[list[Any]],
    date_idx: int,
    value_idx: int,
    ago_idx: int,
    day_offset: int,
    allow_missing_date_values: bool = False,
) -> None:
    def ago_date_callable(cur_date: datetime.date) -> datetime.date:  # noqa
        return cur_date - datetime.timedelta(days=day_offset)

    check_at_date_data(
        data_rows=data_rows,
        date_idx=date_idx,
        value_idx=value_idx,
        ago_idx=ago_idx,
        ago_date_callable=ago_date_callable,
        allow_missing_date_values=allow_missing_date_values,
    )
