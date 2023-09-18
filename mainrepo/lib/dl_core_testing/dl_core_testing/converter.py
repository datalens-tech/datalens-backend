from __future__ import annotations

import csv
import io
from typing import (
    Dict,
    List,
)


def make_csv(headers: List[str], rows: List[Dict[str, str]]) -> str:
    """Turn list of headers and list of data rows into a CSV string"""
    f = io.StringIO()
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return f.getvalue()
