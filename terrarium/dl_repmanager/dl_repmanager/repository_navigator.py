import ast
import os
from pathlib import Path
import re
import sys
from typing import (
    ClassVar,
    Collection,
    Generator,
    Optional,
)

import attr

from dl_repmanager.package_index import PackageIndex
from dl_repmanager.primitives import (
    EntityReference,
    EntityReferenceType,
    ImportSpec,
    PackageInfo,
)
from dl_repmanager.repository_env import RepoEnvironment


@attr.s
class RepositoryNavigator:
    repository_env: RepoEnvironment = attr.ib(kw_only=True)
    package_index: PackageIndex = attr.ib(kw_only=True, factory=PackageIndex)

    _IMPORT_REGEX: ClassVar[re.Pattern] = re.compile(r"^\s*(import|from)\s+(?P<import_name>[\w.]+)")

    @staticmethod
    def recurse_code_files(dir_path: Path) -> Generator[Path, None, None]:
        assert dir_path is not None

        if dir_path.is_file() and str(dir_path).rsplit(".", 1)[1] == "py":
            yield dir_path

        for full_fn in dir_path.rglob("*.py"):
            yield full_fn

    def _file_contains_imports(self, file_path: Path, import_name: str) -> bool:
        import_specs = self._collect_imports_from_file(file_path)
        prefix = f"{import_name}."
        for import_spec in import_specs:
            if import_spec.import_module_name == import_name or import_spec.import_module_name.startswith(prefix):
                return True
        return False

    def recurse_files_with_import(self, import_name: str) -> Generator[Path, None, None]:
        for _package_type, pkg_type_root in self.repository_env.iter_package_abs_dirs():
            for file_path in self.recurse_code_files(pkg_type_root):
                if self._file_contains_imports(file_path, import_name):
                    yield file_path

    def _collect_imports_from_file(self, file_path: Path) -> list[ImportSpec]:
        result: list[ImportSpec] = []
        fs_editor = self.repository_env.get_fs_editor()
        with fs_editor.open(file_path, mode="r") as code_file:
            code_ast = ast.parse(code_file.read())
            for node in ast.walk(code_ast):
                found_modules: set[str]
                if isinstance(node, ast.Import):
                    found_modules = {alias.name for alias in node.names}
                elif isinstance(node, ast.ImportFrom):
                    module_name = node.module
                    if node.level > 0:  # relative import
                        module_name = "." * node.level + (module_name or "")
                    assert module_name is not None
                    found_modules = {module_name}
                else:
                    continue

                for found_module in found_modules:
                    result.append(
                        ImportSpec(
                            import_ast=node,
                            import_module_name=found_module,
                            source_path=file_path,
                        )
                    )

        return result

    def _filter_imports(
        self,
        import_specs: list[ImportSpec],
        mask: Optional[re.Pattern] = None,
        exclude_standard: bool = True,
    ) -> list[ImportSpec]:
        std_modules = set(sys.stdlib_module_names)
        result: list[ImportSpec] = []
        for import_spec in import_specs:
            if exclude_standard and import_spec.import_module_name.split(".")[0] in std_modules:
                continue
            if mask is not None and not mask.match(import_spec.import_module_name):
                continue

            result.append(import_spec)

        return result

    def _collect_imports_from_dir(self, dir_path: Path) -> list[ImportSpec]:
        result: list[ImportSpec] = []
        for file_path in self.recurse_code_files(dir_path):
            result += self._collect_imports_from_file(file_path)

        return result

    def _collect_imports_from_module(self, module_name: str) -> list[ImportSpec]:
        if self.module_is_dir(module_name):
            module_path = self.make_dir_module_path(module_name)
            return self._collect_imports_from_dir(module_path)
        else:
            module_path = self.make_file_module_path(module_name)
            return self._collect_imports_from_file(module_path)

    def collect_import_bases_from_module(
        self,
        module_name: str,
        mask: Optional[re.Pattern] = None,
        exclude_standard: bool = True,
    ) -> set[str]:
        def get_base_package_name(imported_name: str) -> str:
            return imported_name.split(".", 1)[0]

        import_specs = self._collect_imports_from_module(module_name)
        import_specs = self._filter_imports(import_specs, mask=mask, exclude_standard=exclude_standard)

        return {get_base_package_name(import_spec.import_module_name) for import_spec in import_specs}

    def make_file_module_path(self, module_name: str) -> Path:
        path = self.make_dir_module_path(module_name)
        return path.parent / (path.name + ".py")

    def make_dir_module_path(self, module_name: str) -> Path:
        package_module_name = module_name.split(".")[0]
        package_info: PackageInfo
        try:
            package_info = self.package_index.get_package_info_from_module_name(package_module_name=package_module_name)
        except KeyError:
            package_info = self.package_index.get_package_info_from_test_name(test_dir_name=package_module_name)

        package_root = package_info.abs_path
        assert (package_root / package_module_name).exists(), f"Package {package_module_name} does not exist"
        return package_root / module_name.replace(".", os.path.sep)

    def module_is_dir(self, module_name: str) -> bool:
        package_path = self.make_dir_module_path(module_name)
        if package_path.exists() and package_path.is_dir():
            return True

        file_path = self.make_file_module_path(module_name)
        assert file_path.is_file()
        return False

    def _make_module_path(self, module_name: str) -> Path:
        if self.module_is_dir(module_name):
            return self.make_dir_module_path(module_name)
        else:
            return self.make_file_module_path(module_name)

    def _naive_path_to_module(self, path: Path) -> str:
        path_str = str(path)
        assert not path_str.startswith("/") and path_str.endswith(".py")
        return path_str[:-3].replace("/", ".")

    def resolve_entity_to_type(
        self,
        entity_ref: EntityReference,
        to_ref_type: EntityReferenceType,
    ) -> set[EntityReference]:
        result: set[EntityReference]
        match entity_ref.ref_type:
            case EntityReferenceType.package_type:
                if to_ref_type == EntityReferenceType.package_type:
                    return {entity_ref}

                package_info_list = self.package_index.list_package_infos(package_type=entity_ref.name)
                package_entity_refs = {package_info.reg_entity_ref for package_info in package_info_list}
                if to_ref_type == EntityReferenceType.package_type:
                    return package_entity_refs
                else:
                    result = set()
                    for package_entity_ref in package_entity_refs:
                        result.update(
                            self.resolve_entity_to_type(
                                entity_ref=package_entity_ref,
                                to_ref_type=to_ref_type,
                            )
                        )
                    return result

            case EntityReferenceType.package_reg:
                if to_ref_type == EntityReferenceType.package_reg:
                    return {entity_ref}

                package_info = self.package_index.get_package_info_by_reg_name(entity_ref.name)
                if to_ref_type == EntityReferenceType.package_type:
                    return {package_info.package_type_entity_ref}

                if to_ref_type == EntityReferenceType.path:
                    result = set()
                    for module_name in (
                        *package_info.module_names,
                        *package_info.test_dirs,
                    ):
                        result.update(
                            self.resolve_entity_to_type(
                                entity_ref=EntityReference(
                                    ref_type=EntityReferenceType.path,
                                    name=str(package_info.abs_path / module_name),
                                ),
                                to_ref_type=to_ref_type,
                            )
                        )
                    return result

                if to_ref_type == EntityReferenceType.module:
                    result = set()
                    for module_name in package_info.module_names:
                        module_path = package_info.abs_path / module_name
                        for module_sub_path in self.recurse_code_files(module_path):
                            rel_module_sub_path = module_sub_path.relative_to(package_info.abs_path)
                            result.add(
                                EntityReference(
                                    ref_type=EntityReferenceType.module,
                                    name=self._naive_path_to_module(rel_module_sub_path),
                                )
                            )
                    return result

            case EntityReferenceType.path:
                paths = list(self.recurse_code_files(Path(entity_ref.name)))
                if to_ref_type == EntityReferenceType.path:
                    return {EntityReference(ref_type=EntityReferenceType.path, name=str(path)) for path in paths}

                path_to_package_type_and_rel: dict[Path, tuple[str, Path, Path]] = {}
                for path in paths:
                    for (
                        package_type,
                        pkg_type_dir,
                    ) in self.repository_env.iter_package_abs_dirs():
                        try:
                            rel_path = path.relative_to(pkg_type_dir)
                            path_to_package_type_and_rel[path] = (
                                package_type,
                                pkg_type_dir,
                                rel_path,
                            )
                        except ValueError:
                            pass

                if to_ref_type == EntityReferenceType.package_type:
                    return {
                        EntityReference(ref_type=EntityReferenceType.package_type, name=package_type)
                        for package_type, pkg_type_dir, rel_path in path_to_package_type_and_rel.values()
                    }

                path_to_package_info: dict[Path, PackageInfo] = {}
                for path, (
                    _package_type,
                    pkg_type_dir,
                    rel_path,
                ) in path_to_package_type_and_rel.items():
                    package_abs_path = pkg_type_dir / rel_path.parts[0]
                    package_info = self.package_index.get_package_info_from_path(package_abs_path)
                    path_to_package_info[path] = package_info

                if to_ref_type == EntityReferenceType.module:
                    return {
                        EntityReference(
                            ref_type=EntityReferenceType.module,
                            name=self._naive_path_to_module(path.relative_to(package_info.abs_path)),
                        )
                        for path, package_info in path_to_package_info.items()
                    }

                if to_ref_type == EntityReferenceType.package_reg:
                    return {package_info.reg_entity_ref for package_info in path_to_package_info.values()}

            case EntityReferenceType.module:
                # Convert to path and resolve the rest from there
                return self.resolve_entity_to_type(
                    entity_ref=EntityReference(
                        ref_type=EntityReferenceType.path,
                        name=str(self._make_module_path(entity_ref.name)),
                    ),
                    to_ref_type=to_ref_type,
                )

            case _:
                pass

        raise ValueError(f"Unsupported conversion from: {entity_ref.ref_type.name} " f"to {to_ref_type.name}")

    def resolve_entities_to_type(
        self,
        entity_list: Collection[EntityReference],
        to_ref_type: EntityReferenceType,
    ) -> set[EntityReference]:
        result: set[EntityReference] = set()
        for entity_ref in entity_list:
            result.update(self.resolve_entity_to_type(entity_ref=entity_ref, to_ref_type=to_ref_type))
        return result

    def _collect_imports_from_entities(self, entity_list: Collection[EntityReference]) -> list[ImportSpec]:
        path_refs = self.resolve_entities_to_type(entity_list=entity_list, to_ref_type=EntityReferenceType.path)
        paths = sorted({Path(ref.name) for ref in path_refs})
        result: list[ImportSpec] = []
        for path in paths:
            result += self._collect_imports_from_file(path)
        return result

    def collect_imports_of_entities_from_entities(
        self,
        src_entity_list: Collection[EntityReference],
        imported_entity_list: Collection[EntityReference],
    ) -> list[ImportSpec]:
        import_specs = self._collect_imports_from_entities(entity_list=src_entity_list)

        module_refs = self.resolve_entities_to_type(
            entity_list=imported_entity_list, to_ref_type=EntityReferenceType.module
        )
        modules = {ref.name for ref in module_refs}
        module_bases = {module for module in modules if len(module.split(".")) == 1}

        result: list[ImportSpec] = []

        for import_spec in import_specs:
            import_base = import_spec.import_module_name.split(".")[0]
            # Fast check via base
            if import_base in module_bases:
                result.append(import_spec)
                continue
            if import_spec.import_module_name in modules:
                result.append(import_spec)

        return result

    def find_package_by_path(self, path: Path) -> PackageInfo:
        for package_info in self.package_index.list_package_infos():
            if path.is_relative_to(package_info.abs_path):
                return package_info
