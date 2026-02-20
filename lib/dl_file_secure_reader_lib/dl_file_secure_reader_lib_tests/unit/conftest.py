from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def test_types_excel_file() -> Path:
    return Path(__file__).parent / "data" / "excel_types.xlsx"
