from __future__ import annotations

import enum
from os import path
from typing import Optional

import attr
from lxml import etree as et


from datalens_local_dev.get_submodules import rel_path_list_for_pycham_upload
from datalens_local_dev.idea import find_idea_dir


class DirectoryCls(enum.Enum):
    exclude = enum.auto()
    sources_root = enum.auto()
    other = enum.auto()


@attr.s
class DirectoryMarkingManager:
    root: et.ElementTree = attr.ib()

    @classmethod
    def get_iml_path(cls) -> str:
        return path.join(find_idea_dir(), "backend.iml")

    @classmethod
    def get_instance(cls):
        parser = et.XMLParser(remove_blank_text=True)
        tree = et.parse(cls.get_iml_path(), parser)
        return cls(tree)

    def dump(self):
        self.root.write(
            self.get_iml_path(),
            pretty_print=True,
            xml_declaration=True,
            encoding="utf-8",
        )

    def find_main_content_node(self) -> et.Element:
        node = self.root.find(
            "./component[@name='NewModuleRootManager']/content[@url='file://$MODULE_DIR$']"
        )
        if node is None:
            raise ValueError(f"Can not find root content node in {self.get_iml_path()}")
        return node

    @staticmethod
    def url_by_local_path(local_path: str) -> str:
        return f"file://$MODULE_DIR$/{local_path}"

    def get_directory_elem(self, local_path: str) -> Optional[et.Element]:
        all_elem = self.find_main_content_node().findall(
            f"./*[@url='{self.url_by_local_path(local_path)}']"
        )
        if len(all_elem) == 1:
            return all_elem[0]
        if len(all_elem) == 0:
            return None

        raise ValueError(
            f"Unexpected count of directory '{local_path}' = {len(all_elem)}"
        )

    def get_directory_cls(self, local_path: str) -> Optional[DirectoryCls]:
        dir_elem = self.get_directory_elem(local_path)

        if dir_elem is None:
            return None

        if (
            dir_elem.tag == "sourceFolder"
            and dir_elem.attrib["isTestSource"] == "false"
        ):
            return DirectoryCls.sources_root
        if dir_elem.tag == "excludeFolder":
            return DirectoryCls.exclude
        return DirectoryCls.other

    def set_directory_cls(self, local_path: str, cls: Optional[DirectoryCls]):
        dir_elem = self.get_directory_elem(local_path)
        parent = self.find_main_content_node()

        if dir_elem is None:
            idx = 0
        else:
            idx = parent.index(dir_elem)

        if cls == DirectoryCls.exclude:
            new_elem = et.Element(
                "excludeFolder", dict(url=self.url_by_local_path(local_path))
            )
        elif cls == DirectoryCls.sources_root:
            new_elem = et.Element(
                "sourceFolder",
                dict(
                    url=self.url_by_local_path(local_path),
                    isTestSource="false",
                ),
            )
        elif cls is None:
            new_elem = None
        else:
            raise ValueError(f"Can not set dir cls {cls} for {local_path}")

        if dir_elem is not None:
            parent.remove(dir_elem)

        if new_elem is not None:
            parent.insert(idx, new_elem)

    def ensure_directory_cls(self, local_path: str, cls: Optional[DirectoryCls]):
        current_dir_cls = self.get_directory_cls(local_path)
        if current_dir_cls != cls:
            self.set_directory_cls(local_path, cls)


def main():
    ctrl = DirectoryMarkingManager.get_instance()

    submodules = rel_path_list_for_pycham_upload()
    for name in submodules:
        ctrl.ensure_directory_cls(name, DirectoryCls.sources_root)
        ctrl.ensure_directory_cls(f"{name}/.idea", DirectoryCls.exclude)
        ctrl.ensure_directory_cls(f"{name}/build", DirectoryCls.exclude)

    ctrl.ensure_directory_cls("ops/clean_tests_runner", None)
    ctrl.ensure_directory_cls("ops/cloud-deploy", DirectoryCls.sources_root)
    ctrl.ensure_directory_cls("ops/docker-base-images", None)
    ctrl.ensure_directory_cls("lib/testenv-common", None)
    ctrl.ensure_directory_cls("tools", None)
    ctrl.ensure_directory_cls("tools/local_dev", DirectoryCls.sources_root)
    ctrl.ensure_directory_cls("tools/local_dev_2/datalens_local_dev", DirectoryCls.sources_root)
    ctrl.ensure_directory_cls("tools/local_dev/.tools_venv", DirectoryCls.exclude)

    ctrl.dump()
    print("[Pycharm] Ensure project structure done")


if __name__ == "__main__":
    main()
