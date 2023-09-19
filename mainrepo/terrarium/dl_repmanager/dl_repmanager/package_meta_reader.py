from __future__ import annotations

import contextlib
import logging
from pathlib import Path
from typing import (
    Any,
    Generator,
    Iterable,
    Optional,
)

import attr
import tomlkit

from dl_repmanager.fs_editor import FilesystemEditor
from dl_repmanager.primitives import LocaleDomainSpec
from dl_repmanager.toml_tools import (
    TOMLIOFactory,
    TOMLReaderBase,
    TOMLWriter,
)


log = logging.getLogger(__name__)


@attr.s
class PackageMetaReader:
    """
    Class for reading meta info of Python packages.
    It is aware of the project-specific pyproject.toml structure.
    """

    _toml_reader: TOMLReaderBase = attr.ib(kw_only=True)

    _SECTION_NAME_MAIN = "tool.poetry"
    _SECTION_NAME_META = "datalens.meta"  # get this from env?
    _SECTION_NAME_I18N_DOMAINS = "datalens.i18n.domains"

    def _get_main_section(self) -> dict:
        return self._toml_reader.get_section(self._SECTION_NAME_MAIN)

    def _get_meta_section(self) -> dict:
        return self._toml_reader.get_section(self._SECTION_NAME_META, strict=False)

    def get_package_reg_name(self) -> str:
        package_reg_name = self._get_main_section()["name"]
        assert isinstance(package_reg_name, str)
        return package_reg_name

    def get_package_module_names(self) -> tuple[str, ...]:
        module_names = tuple(pkg["include"] for pkg in self._get_main_section()["packages"])  # type: ignore
        return module_names

    def get_package_type(self) -> Optional[str]:  # FIXME: remove `Optional` after it is added to all toml files
        package_reg_name = self._get_meta_section().get("package_type")
        assert package_reg_name is None or isinstance(package_reg_name, str)
        return package_reg_name

    def iter_requirement_items(self, section_name: str) -> Iterable[dict[str, Any]]:
        for key, item in self._toml_reader.iter_section_items(section_name, strict=False):
            if key is None:
                continue

            item_as_dict = {"name": key}
            if isinstance(item.value, str):
                item_as_dict["version"] = item.value
            elif isinstance(item.value, dict):
                item_as_dict.update(item.value)

            yield item_as_dict

    def get_i18n_domains(self) -> tuple[LocaleDomainSpec, ...]:
        result: set[LocaleDomainSpec] = set()
        for domain, item_data in self._toml_reader.iter_section_items(self._SECTION_NAME_I18N_DOMAINS, strict=False):
            domain_str = str(domain).strip()
            paths: list[Path] = []
            for path_data in item_data:
                paths.append(Path(str(path_data["path"]).strip()))

            result.add(LocaleDomainSpec(domain_name=domain_str, scan_paths=tuple(paths)))

        return tuple(result)

    def get_implicit_dependencies(self) -> list[str]:
        meta_section = self._toml_reader.get_section(self._SECTION_NAME_META, strict=False)
        implicit_deps = meta_section.get("implicit_dependencies", ())
        return [item for item in implicit_deps]

    def get_mypy_stubs_overrides(self) -> dict:
        return dict(self._toml_reader.get_section(f"{self._SECTION_NAME_META}.mypy_stubs_packages_override"))

    def get_mypy_common(self) -> dict:
        return dict(self._toml_reader.get_section(f"{self._SECTION_NAME_META}.mypy_common", strict=False))

    def get_mypy_overrides_modules(self) -> list[dict]:
        section = self._toml_reader.get_section("tool.mypy", strict=False)
        return list(section.get("overrides", []))


@attr.s
class PackageMetaWriter(PackageMetaReader):
    @property
    def toml_writer(self) -> TOMLWriter:
        assert isinstance(self._toml_reader, TOMLWriter)
        return self._toml_reader

    def remove_requirement_item(self, section_name: str, item_name: str) -> None:
        with self.toml_writer.suppress_non_existent_key():
            section = self.toml_writer.get_editable_section(section_name)
            section.remove(item_name)

    def _get_item_opt(self, section_name: str, item_name) -> Optional[dict[str, Any]]:
        items = [item for item in self.iter_requirement_items(section_name=section_name) if item["name"] == item_name]
        if not items:
            return None

        assert len(items) == 1
        return items[0]

    def update_requirement_item(
        self,
        section_name: str,
        item_name: str,
        new_item_name: str,
        new_path: Path,
    ) -> None:
        original_item = self._get_item_opt(section_name=section_name, item_name=item_name)
        if original_item is not None:
            section = self.toml_writer.get_editable_section(section_name)
            section.remove(item_name)
            package_dep_table = tomlkit.inline_table()
            package_dep_table.add("path", str(new_path))
            section.add(new_item_name, package_dep_table)

    def add_mypy_overrides_ignore(self, pkg_names: list[str]):
        with self.toml_writer.suppress_non_existent_key():
            section = self.toml_writer.get_editable_section(f"{self._SECTION_NAME_META}.mypy_stubs_packages_override")
            for name in pkg_names:
                override = tomlkit.inline_table()
                override.add("ignore", True)
                section.add(name, override)

    def update_mypy_common(self, to_update: dict[str, str]) -> bool:
        changes = False
        with self.toml_writer.suppress_non_existent_key():
            key = "tools.mypy"
            try:
                section = self.toml_writer.get_editable_section(key)
            except Exception as err:
                print(err)
                section = self.toml_writer.add_section(key)

            for k, v in to_update.items():
                if section[k] != v:
                    changes = True
                section[k] = v
        return changes


@attr.s
class PackageMetaIOFactory:
    fs_editor: FilesystemEditor = attr.ib(kw_only=True)
    toml_io_factory: TOMLIOFactory = attr.ib(init=False)

    @toml_io_factory.default
    def __make_toml_io_factory(self) -> TOMLIOFactory:
        return TOMLIOFactory(fs_editor=self.fs_editor)

    @contextlib.contextmanager
    def package_meta_reader(self, file_path: Path) -> Generator[PackageMetaReader, None, None]:
        with self.toml_io_factory.toml_reader(file_path) as toml_reader:
            yield PackageMetaReader(toml_reader=toml_reader)

    @contextlib.contextmanager
    def package_meta_writer(self, file_path: Path) -> Generator[PackageMetaWriter, None, None]:
        with self.toml_io_factory.toml_writer(file_path) as toml_writer:
            yield PackageMetaWriter(toml_reader=toml_writer)
