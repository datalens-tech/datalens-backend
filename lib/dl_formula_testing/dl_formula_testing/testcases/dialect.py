from __future__ import annotations

from typing import (
    ClassVar,
    Sequence,
)

from dl_formula.core.dialect import (
    DialectCombo,
    DialectName,
    from_name_and_version,
)


class DefaultDialectFormulaConnectorTestSuite:
    dialect_name: ClassVar[DialectName]
    default_dialect: DialectCombo
    dialect_matches: ClassVar[Sequence[tuple[str, DialectCombo]]]

    def test_dialect_versions(self):
        for version_str, expected_dialect in self.dialect_matches:
            assert from_name_and_version(self.dialect_name, version_str) == expected_dialect

    def test_default_dialect(self):
        assert from_name_and_version(self.dialect_name, None) == self.default_dialect
