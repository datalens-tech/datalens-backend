import ast
import os
import re
import sys
from pathlib import Path
from typing import ClassVar, Generator, Optional

import attr

from dl_repmanager.primitives import PackageInfo
from dl_repmanager.env import RepoEnvironment
from dl_repmanager.package_index import PackageIndex


@attr.s
class PackageNavigator:
    repo_env: RepoEnvironment = attr.ib(kw_only=True)
    package_index: PackageIndex = attr.ib(kw_only=True, factory=PackageIndex)

    _IMPORT_REGEX: ClassVar[re.Pattern] = re.compile(r'^\s*(import|from)\s+(?P<import_name>[\w.]+)')

    @staticmethod
    def recurse_code_files(dir_path: Path) -> Generator[Path, None, None]:
        assert dir_path is not None
        for full_fn in dir_path.rglob("*.py"):
            yield full_fn

    def _file_contains_imports(self, file_path: Path, import_name: str) -> bool:
        file_imports = self._collect_imports_from_file(file_path, exclude_standard=False)
        prefix = f'{import_name}.'
        for found_import_name in file_imports:
            if found_import_name == import_name or found_import_name.startswith(prefix):
                return True
        return False

    def recurse_files_with_import(self, import_name: str) -> Generator[Path, None, None]:
        for package_type, pkg_type_root in self.repo_env.iter_package_abs_dirs():
            for file_path in self.recurse_code_files(pkg_type_root):
                if self._file_contains_imports(file_path, import_name):
                    yield file_path

    def _collect_imports_from_file(
            self, file_path: Path, mask: Optional[re.Pattern] = None, exclude_standard: bool = True,
    ) -> set[str]:
        result: set[str] = set()
        std_modules = set(sys.stdlib_module_names)
        fs_editor = self.repo_env.get_fs_editor()
        with fs_editor.open(file_path, mode='r') as code_file:
            code_ast = ast.parse(code_file.read())
            for node in ast.walk(code_ast):
                found_modules: set[str]
                if isinstance(node, ast.Import):
                    found_modules = {alias.name for alias in node.names}
                elif isinstance(node, ast.ImportFrom):
                    module_name = node.module
                    if node.level > 0:  # relative import
                        module_name = '.'*node.level + (module_name or '')
                    assert module_name is not None
                    found_modules = {module_name}
                else:
                    continue

                for found_module in found_modules:
                    if exclude_standard and found_module.split('.')[0] in std_modules:
                        continue
                    if mask is not None and not mask.match(found_module):
                        continue

                    result.add(found_module)

        return result

    def _collect_imports_from_dir(
            self, dir_path: Path, mask: Optional[re.Pattern] = None, exclude_standard: bool = True,
    ) -> set[str]:
        result: set[str] = set()
        for file_path in self.recurse_code_files(dir_path):
            result |= self._collect_imports_from_file(file_path, mask=mask, exclude_standard=exclude_standard)

        return result

    def collect_imports_from_module(
            self, module_name: str, mask: Optional[re.Pattern] = None, exclude_standard: bool = True,
    ) -> set[str]:
        if self.module_is_package(module_name):
            module_path = self.make_package_module_path(module_name)
            return self._collect_imports_from_dir(module_path, mask=mask, exclude_standard=exclude_standard)
        else:
            module_path = self.make_file_module_path(module_name)
            return self._collect_imports_from_file(module_path, mask=mask, exclude_standard=exclude_standard)

    def collect_import_bases_from_module(
            self, module_name: str, mask: Optional[re.Pattern] = None, exclude_standard: bool = True,
    ) -> set[str]:
        def get_base_package_name(imported_name: str) -> str:
            return imported_name.split('.', 1)[0]

        return {
            get_base_package_name(imported_name)
            for imported_name in self.collect_imports_from_module(
                module_name, mask=mask, exclude_standard=exclude_standard,
            )
        }

    def make_file_module_path(self, module_name: str) -> Path:
        path = self.make_package_module_path(module_name)
        return path.parent / (path.name + ".py")

    def make_package_module_path(self, module_name: str) -> Path:
        package_module_name = module_name.split('.')[0]
        package_info: PackageInfo
        try:
            package_info = self.package_index.get_package_info_from_module_name(
                package_module_name=package_module_name)
        except KeyError:
            package_info = self.package_index.get_package_info_from_test_name(
                test_dir_name=package_module_name)

        package_root = package_info.abs_path
        assert (package_root / package_module_name).exists(), (
            f'Package {package_module_name} does not exist'
        )
        return package_root / module_name.replace('.', os.path.sep)

    def module_is_package(self, module_name: str) -> bool:
        package_path = self.make_package_module_path(module_name)
        if package_path.exists() and package_path.is_dir():
            return True

        file_path = self.make_file_module_path(module_name)
        assert file_path.is_file()
        return False
