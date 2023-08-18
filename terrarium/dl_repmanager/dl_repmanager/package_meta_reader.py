from __future__ import annotations

import contextlib
from typing import Any, Generator, Iterable, Optional

import attr

from dl_repmanager.toml_tools import TOMLReader


@attr.s
class PackageMetaReader:
    """
    Class for reading meta info of Python packages.
    It is aware of the project-specific pyproject.toml structure.
    """

    _toml_reader: TOMLReader = attr.ib(kw_only=True)

    _SECTION_NAME_MAIN = 'tool.poetry'
    _SECTION_NAME_META = 'datalens.meta'  # get this from env?
    _SECTION_NAME_I18N_DOMAINS = 'datalens.i18n.domains'

    @classmethod
    @contextlib.contextmanager
    def from_file(cls, filename: str) -> Generator[PackageMetaReader, None, None]:
        with TOMLReader.from_file(filename) as toml_reader:
            yield PackageMetaReader(toml_reader=toml_reader)

    def _get_main_section(self) -> dict:
        return self._toml_reader.get_section(self._SECTION_NAME_MAIN)

    def _get_meta_section(self) -> dict:
        return self._toml_reader.get_section(self._SECTION_NAME_META, strict=False)

    def get_package_reg_name(self) -> str:
        package_reg_name = self._get_main_section()['name']
        assert isinstance(package_reg_name, str)
        return package_reg_name

    def get_package_module_names(self) -> tuple[str, ...]:
        module_names = tuple(pkg['include'] for pkg in self._get_main_section()['packages'])  # type: ignore
        return module_names

    def get_package_type(self) -> Optional[str]:  # FIXME: remove `Optional` after it is added to all toml files
        package_reg_name = self._get_meta_section().get('package_type')
        assert package_reg_name is None or isinstance(package_reg_name, str)
        return package_reg_name

    def iter_requirement_items(self, section_name: str) -> Iterable[dict[str, Any]]:
        for key, item in self._toml_reader.iter_section_items(section_name, strict=False):
            if key is None:
                continue

            item_as_dict = {'name': key.key}
            if isinstance(item.value, str):
                item_as_dict['version'] = item.value
            elif isinstance(item.value, dict):
                item_as_dict.update(item.value)

            yield item_as_dict

    def get_i18n_domains(self) -> dict[str, str]:
        result: dict[str, str] = {}
        for domain, path in self._toml_reader.iter_section_items(self._SECTION_NAME_I18N_DOMAINS, strict=False):
            result[str(domain).strip()] = str(path).strip()

        return result
