from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any, Generator, Iterable

import attr
import tomlkit
from tomlkit.toml_document import TOMLDocument
from tomlkit.items import Item as TOMLItem, AbstractTable
from tomlkit.container import Container as TOMLContainer, OutOfOrderTableProxy
from tomlkit.exceptions import NonExistentKey


@attr.s
class TOMLReaderBase:
    _toml: TOMLDocument = attr.ib(kw_only=True)

    def get_section(self, key: str, strict: bool = True) -> dict:
        section: dict = self._toml
        item: TOMLContainer | TOMLItem | OutOfOrderTableProxy
        for part in key.split('.'):
            try:
                item = section[part]
            except KeyError:
                if strict:
                    raise
                item = tomlkit.table()

            assert isinstance(item, dict)
            section = item

        assert isinstance(section, dict)
        return section

    def iter_section_items(self, key: str, strict: bool = True) -> Iterable[tuple[Any, Any]]:
        section = self.get_section(key=key, strict=strict)
        assert isinstance(section, AbstractTable)
        for item_key, item in section.value.body:
            yield item_key, item


class TOMLReader(TOMLReaderBase):
    """Generic pyproject.toml reader"""

    @classmethod
    @contextlib.contextmanager
    def from_file(cls, file_path: Path) -> Generator[TOMLReader, None, None]:
        with open(file_path) as f:
            yield TOMLReader(toml=tomlkit.load(f))


class TOMLWriter(TOMLReaderBase):
    """Generic pyproject.toml writer"""

    @classmethod
    @contextlib.contextmanager
    def from_file(cls, file_path: Path) -> Generator[TOMLWriter, None, None]:
        with open(file_path, 'r+') as f:
            toml = tomlkit.load(f)
            yield TOMLWriter(toml=toml)
            f.seek(0)
            f.truncate(0)
            f.write(tomlkit.dumps(toml))

    def get_editable_section(self, key: str) -> AbstractTable:
        # None of this makes any sense. This is just the way the tomlkit library is.
        # __getitem__ might return an `OutOfOrderTableProxy` or an `AbstractTable` - you never know.
        # `AbstractTable` does have an `add` method while `OutOfOrderTableProxy` doesn't.
        section = self.get_section(key)
        assert isinstance(section, AbstractTable)
        return section

    def add_section(self, key: str) -> AbstractTable:
        section: dict = self._toml
        item: TOMLItem | TOMLContainer | OutOfOrderTableProxy
        for part in key.split('.'):
            try:
                item = section[part]
            except KeyError:
                section[part] = tomlkit.table()
                item = section[part]

            assert isinstance(item, dict)
            section = item

        assert isinstance(section, AbstractTable)
        return section

    def delete_section(self, key: str) -> None:
        section: dict = self._toml
        parts = key.split('.')
        for part in parts[:-1]:
            try:
                item = section[part]
            except KeyError:
                # Parent section doesn't exist
                return

            assert isinstance(item, dict)
            section = item

        del section[parts[-1]]

    @classmethod
    @contextlib.contextmanager
    def suppress_non_existent_key(cls) -> Generator[None, None, None]:
        try:
            yield
        except NonExistentKey:
            pass
